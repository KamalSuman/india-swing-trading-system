from __future__ import annotations

import ast
import inspect
import unittest
from dataclasses import replace
from datetime import datetime, time, timedelta, tzinfo
from decimal import Decimal

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.market_data.models import DailyCandle, DailyCandleBatch, NseSessionFinality
from india_swing.quality_pilot import resumable_service as service_module
from india_swing.quality_pilot.campaign_ledger import QualityPilotCampaignCompletenessLedger
from india_swing.quality_pilot.canonical_response import EndpointFamily, ResponseClassification
from india_swing.quality_pilot.capture_runner import QualityPilotCollectionResult
from india_swing.quality_pilot.control_plane_store import (
    PinnedQualityPilotControlArtifactRequest,
    QualityPilotControlArtifactKind,
    build_quality_pilot_completeness_snapshot,
    decode_quality_pilot_completeness_snapshot,
    encode_quality_pilot_completeness_snapshot,
    pinned_quality_pilot_ledger_transition_request,
    publish_quality_pilot_control_artifact,
)
from india_swing.quality_pilot.resumable_service import (
    QUALITY_PILOT_RESUMABLE_REQUEST_SCHEMA_VERSION,
    QUALITY_PILOT_RESUMABLE_RESULT_SCHEMA_VERSION,
    QualityPilotResumableCaptureRequest,
    QualityPilotResumableCaptureResult,
    QualityPilotResumableCaptureService,
    QualityPilotResumableServiceError,
    audit_replay_quality_pilot_completeness_snapshot,
)
from tests.test_quality_pilot_campaign_ledger import BUCKET, _payload, _plan, _run
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter


class FakeReader:
    """Fake GCSObjectReader over a shared in-memory (bucket, object, generation) store."""

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
    """FakeStateObjectWriter that also mirrors every write into a reader-visible store."""

    def __init__(self, reader_store: dict) -> None:
        super().__init__()
        self.reader_store = reader_store

    def create_or_verify(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
        result = super().create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=content_bytes,
            content_type=content_type,
            maximum_bytes=maximum_bytes,
        )
        if self.malicious_result is None and self.raise_error is None:
            stored_bytes, _ = self.store[(bucket, object_name)]
            self.reader_store[(bucket, object_name, result.generation)] = stored_bytes
        return result


def _pin(published, **overrides) -> PinnedQualityPilotControlArtifactRequest:
    kwargs = dict(
        storage_policy_version=published.storage_policy_version,
        protocol_sha256=published.protocol_sha256,
        kind=published.kind,
        pilot_run_id=published.pilot_run_id,
        artifact_id=published.artifact_id,
        bucket=published.bucket,
        object_name=published.object_name,
        generation=published.generation,
        expected_encoded_sha256=published.encoded_sha256,
    )
    kwargs.update(overrides)
    return PinnedQualityPilotControlArtifactRequest(**kwargs)


class SequenceCollector:
    """Fake QualityPilotCollector. For DAILY_OHLCV specs, ``observed_at`` tracks
    whatever start time this collector actually uses (never a fixture-fixed
    value), so chained same-window OHLCV chunks stay internally consistent."""

    def __init__(self, *, offset_seconds: int = 1, not_before: datetime | None = None) -> None:
        self.calls = 0
        self.offset_seconds = offset_seconds
        self.not_before = not_before
        self.error: Exception | None = None

    def collect(self, spec):
        self.calls += 1
        if self.error is not None:
            raise self.error
        start = spec.window.opens_at
        if self.not_before is not None and self.not_before > start:
            start = self.not_before
        if spec.window.endpoint_family is EndpointFamily.DAILY_OHLCV:
            finality = NseSessionFinality.regular_collection_guard(spec.window.market_session)
            candle = DailyCandle(
                instrument_token=spec.provider_instrument_token,
                timestamp=datetime.combine(spec.window.market_session, time(0, 0), tzinfo=INDIA_STANDARD_TIME),
                open=Decimal("1490.00"),
                high=Decimal("1510.00"),
                low=Decimal("1480.00"),
                close=Decimal("1500.00"),
                volume=1000,
                open_interest=None,
            )
            payload = DailyCandleBatch(
                instrument_token=spec.provider_instrument_token,
                session_finality=finality,
                observed_at=start,
                provider_version="kite-3.0",
                candles=(candle,),
            )
        else:
            payload = _payload(spec)
        return QualityPilotCollectionResult(
            request_started_at=start,
            request_ended_at=start + timedelta(seconds=self.offset_seconds),
            response_classification=ResponseClassification.SUCCESS,
            payload=payload,
        )


class Fixture:
    """One published plan plus a shared reader/writer store, ready for genesis calls."""

    def __init__(self) -> None:
        self.plan = _plan()
        self.reader_store: dict = {}
        self.writer = SharedFakeWriter(self.reader_store)
        self.published_plan = publish_quality_pilot_control_artifact(self.plan, BUCKET, self.writer)
        self.plan_pin = _pin(self.published_plan)
        self.service = QualityPilotResumableCaptureService()

    def reader(self) -> FakeReader:
        return FakeReader(self.reader_store)

    def genesis_request(self, **overrides) -> QualityPilotResumableCaptureRequest:
        first_spec = self.plan.capture_specs[0]
        kwargs = dict(
            plan_pin=self.plan_pin,
            predecessor_transition_pin=None,
            target_capture_spec_id=first_spec.capture_spec_id,
            invocation_at=first_spec.window.opens_at,
        )
        kwargs.update(overrides)
        return QualityPilotResumableCaptureRequest(**kwargs)

    def run_genesis(self, collector: SequenceCollector | None = None) -> QualityPilotResumableCaptureResult:
        return self.service.run(self.genesis_request(), self.reader(), collector or SequenceCollector(), self.writer)

    def resume_request(self, predecessor_result: QualityPilotResumableCaptureResult, index: int, **overrides):
        spec = self.plan.capture_specs[index]
        transition_pin = pinned_quality_pilot_ledger_transition_request(predecessor_result.published_transition)
        kwargs = dict(
            plan_pin=self.plan_pin,
            predecessor_transition_pin=transition_pin,
            target_capture_spec_id=spec.capture_spec_id,
            invocation_at=spec.window.opens_at,
        )
        kwargs.update(overrides)
        return QualityPilotResumableCaptureRequest(**kwargs)

    def resume(self, predecessor_result, index, *, collector=None, **request_overrides):
        request = self.resume_request(predecessor_result, index, **request_overrides)
        return self.service.run(request, self.reader(), collector or SequenceCollector(), self.writer)

    def advance_chain(self, count: int) -> QualityPilotResumableCaptureResult:
        """Genesis plus ``count - 1`` resumes, correctly timed across shared
        OHLCV windows, returning the final result."""

        current = self.run_genesis(SequenceCollector(offset_seconds=1))
        last_evaluated_at = self.plan.capture_specs[0].window.opens_at + timedelta(seconds=1)
        for index in range(1, count):
            spec = self.plan.capture_specs[index]
            invocation_at = max(spec.window.opens_at, last_evaluated_at)
            current = self.resume(
                current,
                index,
                collector=SequenceCollector(offset_seconds=1, not_before=invocation_at),
                invocation_at=invocation_at,
            )
            last_evaluated_at = invocation_at + timedelta(seconds=1)
        return current


class GenesisHappyPathTests(unittest.TestCase):
    def test_genesis_reads_plan_once_writes_thrice_and_fully_verifies(self) -> None:
        fixture = Fixture()
        reader = fixture.reader()
        collector = SequenceCollector()
        writer_calls_before = len(fixture.writer.calls)
        result = fixture.service.run(fixture.genesis_request(), reader, collector, fixture.writer)

        self.assertEqual(collector.calls, 1)
        self.assertEqual(len(reader.calls), 1)
        self.assertEqual(len(fixture.writer.calls) - writer_calls_before, 3)
        result.verify_content_identity()
        self.assertIsNone(result.previous_snapshot_id)
        self.assertEqual(result.plan_id, fixture.plan.plan_id)
        self.assertEqual(result.capture_spec_id, fixture.plan.capture_specs[0].capture_spec_id)

        self.assertEqual(result.published_snapshot.kind, QualityPilotControlArtifactKind.COMPLETENESS_LEDGER)
        self.assertEqual(result.transition.previous_snapshot_id, None)
        self.assertEqual(result.transition.capture_spec_id, fixture.plan.capture_specs[0].capture_spec_id)
        self.assertTrue(result.quality_only)
        self.assertFalse(result.capital_eligible)
        self.assertFalse(result.paper_trade_eligible)

    def test_genesis_snapshot_contains_one_completed_prefix_item(self) -> None:
        fixture = Fixture()
        result = fixture.run_genesis()
        key = (result.published_snapshot.bucket, result.published_snapshot.object_name, result.published_snapshot.generation)
        snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])
        self.assertEqual(len(snapshot.completed_capture_spec_ids), 1)
        self.assertEqual(snapshot.completed_capture_spec_ids[0], fixture.plan.capture_specs[0].capture_spec_id)
        self.assertEqual(len(snapshot.pinned_observations), 1)
        self.assertGreater(snapshot.published_observation_byte_counts[0], 0)
        self.assertTrue(snapshot.quality_only)
        self.assertFalse(snapshot.capital_eligible)


class ResumeHappyPathTests(unittest.TestCase):
    def test_resume_after_fifty_completed_captures_uses_exactly_three_reads(self) -> None:
        fixture = Fixture()
        predecessor = fixture.advance_chain(50)

        target_spec = fixture.plan.capture_specs[50]
        transition_pin = pinned_quality_pilot_ledger_transition_request(predecessor.published_transition)
        invocation_at = max(target_spec.window.opens_at, _evaluated_at_of(fixture, predecessor))
        request = QualityPilotResumableCaptureRequest(
            plan_pin=fixture.plan_pin,
            predecessor_transition_pin=transition_pin,
            target_capture_spec_id=target_spec.capture_spec_id,
            invocation_at=invocation_at,
        )
        reader = fixture.reader()
        collector = SequenceCollector(offset_seconds=1, not_before=invocation_at)
        writer_calls_before = len(fixture.writer.calls)
        result = fixture.service.run(request, reader, collector, fixture.writer)

        self.assertEqual(len(reader.calls), 3)
        self.assertEqual(collector.calls, 1)
        self.assertEqual(len(fixture.writer.calls) - writer_calls_before, 3)
        result.verify_content_identity()
        self.assertEqual(result.previous_snapshot_id, predecessor.snapshot_id)

        key = (result.published_snapshot.bucket, result.published_snapshot.object_name, result.published_snapshot.generation)
        snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])
        self.assertEqual(len(snapshot.completed_capture_spec_ids), 51)

    def test_transition_binds_predecessor_to_successor(self) -> None:
        fixture = Fixture()
        genesis = fixture.run_genesis()
        result = fixture.resume(genesis, 1)
        self.assertEqual(result.transition.previous_snapshot_id, genesis.snapshot_id)
        self.assertEqual(result.transition.capture_spec_id, fixture.plan.capture_specs[1].capture_spec_id)
        self.assertNotEqual(result.published_transition.object_name, genesis.published_transition.object_name)


def _evaluated_at_of(fixture: Fixture, result: QualityPilotResumableCaptureResult) -> datetime:
    key = (result.published_snapshot.bucket, result.published_snapshot.object_name, result.published_snapshot.generation)
    snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])
    return snapshot.evaluated_at


class IncrementalAdvancementEquivalenceTests(unittest.TestCase):
    """Prove _advance_completeness_snapshot is byte-for-byte and ID-equivalent
    to build_quality_pilot_completeness_snapshot(QualityPilotCampaignCompletenessLedger(...))
    at every step of a representative chain spanning catalog, quote, OHLCV,
    a classified gap, a session boundary, pending/due-incomplete states, and
    the fully complete campaign."""

    def test_every_step_of_a_two_session_chain_matches_the_full_ledger_derivation(self) -> None:
        fixture = Fixture()
        plan = fixture.plan
        run_results = []
        current = None

        def gap_collector(classification):
            def _collect(spec):
                return QualityPilotCollectionResult(
                    request_started_at=spec.window.opens_at,
                    request_ended_at=spec.window.opens_at + timedelta(seconds=1),
                    response_classification=classification,
                    payload=None,
                )

            class _C:
                def collect(self, spec):
                    return _collect(spec)

            return _C()

        last_evaluated_at = plan.capture_specs[0].window.opens_at
        for index in range(10):  # two full sessions (5 specs each)
            spec = plan.capture_specs[index]
            invocation_at = max(spec.window.opens_at, last_evaluated_at)
            if index == 2:
                # Introduce a classified gap for the QUOTE_CLOSE capture of
                # session 0 (endpoint-compatible: PROVIDER_GAP for FULL_QUOTE).
                collector = gap_collector(ResponseClassification.PROVIDER_GAP)
            else:
                collector = SequenceCollector(offset_seconds=1, not_before=invocation_at)
            if current is None:
                request = QualityPilotResumableCaptureRequest(
                    plan_pin=fixture.plan_pin, predecessor_transition_pin=None,
                    target_capture_spec_id=spec.capture_spec_id, invocation_at=invocation_at,
                )
            else:
                request = fixture.resume_request(current, index, invocation_at=invocation_at)
            current = fixture.service.run(request, fixture.reader(), collector, fixture.writer)
            run_results.append(_run_result_for(fixture, current))
            last_evaluated_at = invocation_at + timedelta(seconds=1)

            with self.subTest(step=index):
                evaluated_at = run_results[-1].observation.request.request_ended_at
                expected_ledger = QualityPilotCampaignCompletenessLedger(
                    plan, tuple(run_results), evaluated_at, BUCKET
                )
                expected_snapshot = build_quality_pilot_completeness_snapshot(expected_ledger)
                key = (
                    current.published_snapshot.bucket,
                    current.published_snapshot.object_name,
                    current.published_snapshot.generation,
                )
                actual_snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])
                self.assertEqual(actual_snapshot.snapshot_id, expected_snapshot.snapshot_id)
                self.assertEqual(
                    encode_quality_pilot_completeness_snapshot(actual_snapshot),
                    encode_quality_pilot_completeness_snapshot(expected_snapshot),
                )
                self.assertEqual(actual_snapshot.status, expected_snapshot.status)

    def test_final_step_reaches_outcomes_complete_and_matches_full_derivation(self) -> None:
        fixture = Fixture()
        plan = fixture.plan
        final = fixture.advance_chain(len(plan.capture_specs))
        key = (
            final.published_snapshot.bucket,
            final.published_snapshot.object_name,
            final.published_snapshot.generation,
        )
        actual_snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])

        # audit_replay_quality_pilot_completeness_snapshot independently
        # reloads every prior observation and already asserts byte-identical
        # reproduction internally; reuse its returned run_results here to
        # additionally assert exact status/id equality against a freshly
        # built full-ledger derivation, for a completely independent
        # construction path.
        run_results = audit_replay_quality_pilot_completeness_snapshot(plan, actual_snapshot, fixture.reader())
        expected_ledger = QualityPilotCampaignCompletenessLedger(
            plan, run_results, actual_snapshot.evaluated_at, BUCKET
        )
        expected_snapshot = build_quality_pilot_completeness_snapshot(expected_ledger)
        self.assertEqual(actual_snapshot.snapshot_id, expected_snapshot.snapshot_id)
        self.assertEqual(
            encode_quality_pilot_completeness_snapshot(actual_snapshot),
            encode_quality_pilot_completeness_snapshot(expected_snapshot),
        )

        from india_swing.quality_pilot.campaign_ledger import CampaignCompletenessStatus

        self.assertEqual(actual_snapshot.status, CampaignCompletenessStatus.OUTCOMES_COMPLETE)


def _run_result_for(fixture: Fixture, result: QualityPilotResumableCaptureResult):
    """Reconstruct the exact QualityPilotCaptureRunResult behind one resumable result."""

    from india_swing.quality_pilot.observation_store import (
        QUALITY_OBSERVATION_STORE_POLICY_VERSION,
        LoadedQualityPilotObservation,
        PublishedQualityPilotObservation,
        read_pinned_quality_pilot_observation,
    )
    from india_swing.quality_pilot.capture_runner import QualityPilotCaptureRunResult

    key = (result.published_snapshot.bucket, result.published_snapshot.object_name, result.published_snapshot.generation)
    snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])
    pin = snapshot.pinned_observations[-1]
    byte_count = snapshot.published_observation_byte_counts[-1]
    loaded = read_pinned_quality_pilot_observation(pin, fixture.reader())
    observation = loaded.observation
    published = PublishedQualityPilotObservation(
        storage_policy_version=QUALITY_OBSERVATION_STORE_POLICY_VERSION,
        protocol_sha256=observation.request.protocol_sha256,
        observation_id=pin.expected_observation_id,
        pilot_run_id=pin.pilot_run_id,
        market_session=pin.market_session,
        window_kind=pin.window_kind,
        endpoint_family=pin.endpoint_family,
        chunk_index=pin.chunk_index,
        chunk_count=pin.chunk_count,
        bucket=pin.bucket,
        object_name=pin.object_name,
        generation=pin.generation,
        encoded_byte_count=byte_count,
        encoded_sha256=pin.expected_encoded_sha256,
    )
    index = len(snapshot.completed_capture_spec_ids) - 1
    spec = fixture.plan.capture_specs[index]
    calendar_decision_id = fixture.plan.campaign.calendar_decision_ids[
        fixture.plan.campaign.confirmed_sessions.index(spec.window.market_session)
    ]
    return QualityPilotCaptureRunResult(
        campaign=fixture.plan.campaign,
        capture_spec=spec,
        campaign_id=fixture.plan.campaign.campaign_id,
        capture_spec_id=spec.capture_spec_id,
        requested_bucket=snapshot.bucket,
        calendar_decision_id=calendar_decision_id,
        observation=observation,
        published=published,
    )


class PreCollectorGateTests(unittest.TestCase):
    """Every case here must never call the collector or the writer."""

    def _assert_rejected_before_collector(self, fixture: Fixture, request, collector=None) -> None:
        collector = collector or SequenceCollector()
        writer_calls_before = len(fixture.writer.calls)
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.service.run(request, fixture.reader(), collector, fixture.writer)
        self.assertEqual(collector.calls, 0)
        self.assertEqual(len(fixture.writer.calls), writer_calls_before)

    def test_rejects_empty_plan(self) -> None:
        fixture = Fixture()
        empty_plan = replace(fixture.plan, capture_specs=())
        writer = SharedFakeWriter(fixture.reader_store)
        published_empty = publish_quality_pilot_control_artifact(empty_plan, BUCKET, writer)
        empty_pin = _pin(published_empty)
        request = QualityPilotResumableCaptureRequest(
            plan_pin=empty_pin,
            predecessor_transition_pin=None,
            target_capture_spec_id=fixture.plan.capture_specs[0].capture_spec_id,
            invocation_at=fixture.plan.capture_specs[0].window.opens_at,
        )
        collector = SequenceCollector()
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.service.run(request, FakeReader(fixture.reader_store), collector, writer)
        self.assertEqual(collector.calls, 0)

    def test_rejects_absent_predecessor_targeting_non_first_spec(self) -> None:
        fixture = Fixture()
        request = fixture.genesis_request(target_capture_spec_id=fixture.plan.capture_specs[1].capture_spec_id)
        self._assert_rejected_before_collector(fixture, request)

    def test_rejects_predecessor_targeting_already_completed_spec(self) -> None:
        fixture = Fixture()
        genesis = fixture.run_genesis()
        request = fixture.resume_request(genesis, 0)
        self._assert_rejected_before_collector(fixture, request)

    def test_rejects_predecessor_targeting_skipped_spec(self) -> None:
        fixture = Fixture()
        genesis = fixture.run_genesis()
        request = fixture.resume_request(genesis, 3)
        self._assert_rejected_before_collector(fixture, request)

    def test_rejects_malformed_target_id(self) -> None:
        fixture = Fixture()
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.genesis_request(target_capture_spec_id="not-a-sha256")

    def test_rejects_plan_transition_bucket_mismatch(self) -> None:
        fixture = Fixture()
        genesis = fixture.run_genesis()
        real_transition_pin = pinned_quality_pilot_ledger_transition_request(genesis.published_transition)
        mismatched_bucket_pin = replace(real_transition_pin, bucket="a-different-quality-pilot-bucket")
        with self.assertRaises(QualityPilotResumableServiceError):
            QualityPilotResumableCaptureRequest(
                plan_pin=fixture.plan_pin,
                predecessor_transition_pin=mismatched_bucket_pin,
                target_capture_spec_id=fixture.plan.capture_specs[1].capture_spec_id,
                invocation_at=fixture.plan.capture_specs[1].window.opens_at,
            )

    def test_rejects_invocation_before_target_window(self) -> None:
        fixture = Fixture()
        first = fixture.plan.capture_specs[0]
        request = fixture.genesis_request(invocation_at=first.window.opens_at - timedelta(seconds=1))
        self._assert_rejected_before_collector(fixture, request)

    def test_rejects_invocation_after_target_window(self) -> None:
        fixture = Fixture()
        first = fixture.plan.capture_specs[0]
        request = fixture.genesis_request(invocation_at=first.window.closes_at + timedelta(seconds=1))
        self._assert_rejected_before_collector(fixture, request)

    def test_rejects_invocation_earlier_than_predecessor_evaluated_at(self) -> None:
        fixture = Fixture()
        result = fixture.advance_chain(4)  # completes specs 0..3 (the first OHLCV chunk)
        # Specs 3 and 4 share one window. Target spec 4 with an invocation_at
        # that is inside that window but earlier than spec 3's own evaluated_at.
        self.assertEqual(fixture.plan.capture_specs[3].window, fixture.plan.capture_specs[4].window)
        early_invocation = fixture.plan.capture_specs[4].window.opens_at
        request = fixture.resume_request(result, 4, invocation_at=early_invocation)
        collector = SequenceCollector()
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.service.run(request, fixture.reader(), collector, fixture.writer)
        self.assertEqual(collector.calls, 0)

    def test_rejects_non_aware_invocation_at(self) -> None:
        fixture = Fixture()
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.genesis_request(invocation_at=datetime(2026, 8, 3, 9, 0))

    def test_rejects_hostile_tzinfo_invocation_at(self) -> None:
        secret = "SECRET-INVOCATION-TZ/C:/private"

        class HostileTimezone(tzinfo):
            def utcoffset(self, dt):
                raise RuntimeError(secret)

            def dst(self, dt):
                return timedelta(0)

        fixture = Fixture()
        hostile_value = datetime(2026, 8, 3, 9, 0, tzinfo=HostileTimezone())
        with self.assertRaises(QualityPilotResumableServiceError) as raised:
            fixture.genesis_request(invocation_at=hostile_value)
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_rejects_tampered_request_id(self) -> None:
        fixture = Fixture()
        request = fixture.genesis_request()
        object.__setattr__(request, "request_id", "0" * 64)
        self._assert_rejected_before_collector(fixture, request)

    def test_tampering_request_posture_via_setattr_is_structurally_impossible(self) -> None:
        fixture = Fixture()
        request = fixture.genesis_request()
        with self.assertRaises(AttributeError):
            object.__setattr__(request, "quality_only", False)


class PredecessorGateTests(unittest.TestCase):
    """These gates now operate purely on the sealed transition's next_snapshot
    metadata and the pinned predecessor snapshot -- never a full replay."""

    def test_rejects_predecessor_snapshot_that_is_not_a_canonical_prefix(self) -> None:
        # A genuinely self-consistent, correctly-hashed, real snapshot whose
        # sole completed outcome is spec[1] (skipping spec[0]).
        fixture = Fixture()
        skip_first_run = _run(fixture.plan.capture_specs[1])
        skip_ledger = QualityPilotCampaignCompletenessLedger(
            fixture.plan, (skip_first_run,), skip_first_run.observation.request.request_ended_at, BUCKET
        )
        skip_snapshot = build_quality_pilot_completeness_snapshot(skip_ledger)
        published_skip_snapshot = publish_quality_pilot_control_artifact(skip_snapshot, BUCKET, fixture.writer)

        # Seal a real transition whose next_snapshot is this non-prefix snapshot.
        from india_swing.quality_pilot.control_plane_store import (
            QualityPilotLedgerTransition,
            publish_quality_pilot_ledger_transition,
        )

        bad_transition = QualityPilotLedgerTransition(
            protocol_sha256=fixture.plan.campaign.protocol_sha256,
            pilot_run_id=fixture.plan.campaign.pilot_run_id,
            plan_id=fixture.plan.plan_id,
            previous_snapshot_id=None,
            capture_spec_id=fixture.plan.capture_specs[1].capture_spec_id,
            run_result_id=skip_first_run.run_result_id,
            next_snapshot=published_skip_snapshot,
        )
        published_bad_transition = publish_quality_pilot_ledger_transition(bad_transition, BUCKET, fixture.writer)
        bad_transition_pin = pinned_quality_pilot_ledger_transition_request(published_bad_transition)

        request = QualityPilotResumableCaptureRequest(
            plan_pin=fixture.plan_pin,
            predecessor_transition_pin=bad_transition_pin,
            target_capture_spec_id=fixture.plan.capture_specs[2].capture_spec_id,
            invocation_at=fixture.plan.capture_specs[2].window.opens_at,
        )
        collector = SequenceCollector()
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.service.run(request, fixture.reader(), collector, fixture.writer)
        self.assertEqual(collector.calls, 0)

    def test_rejects_transition_whose_snapshot_cannot_be_loaded(self) -> None:
        fixture = Fixture()
        genesis = fixture.run_genesis()
        # Remove the predecessor snapshot bytes from the reader-visible store.
        key = (genesis.published_snapshot.bucket, genesis.published_snapshot.object_name, genesis.published_snapshot.generation)
        del fixture.reader_store[key]
        request = fixture.resume_request(genesis, 1)
        collector = SequenceCollector()
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.service.run(request, fixture.reader(), collector, fixture.writer)
        self.assertEqual(collector.calls, 0)

    def test_rejects_corrupted_predecessor_snapshot_bytes(self) -> None:
        fixture = Fixture()
        genesis = fixture.run_genesis()
        key = (genesis.published_snapshot.bucket, genesis.published_snapshot.object_name, genesis.published_snapshot.generation)
        fixture.reader_store[key] = b"not-json-at-all"
        request = fixture.resume_request(genesis, 1)
        collector = SequenceCollector()
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.service.run(request, fixture.reader(), collector, fixture.writer)
        self.assertEqual(collector.calls, 0)


class PostCollectorGateTests(unittest.TestCase):
    def test_collector_exception_is_sanitized_and_never_publishes(self) -> None:
        secret = "SECRET-RESUMABLE-COLLECTOR/C:/private"
        fixture = Fixture()
        collector = SequenceCollector()
        collector.error = RuntimeError(secret)
        writer_calls_before = len(fixture.writer.calls)
        with self.assertRaises(QualityPilotResumableServiceError) as raised:
            fixture.service.run(fixture.genesis_request(), fixture.reader(), collector, fixture.writer)
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertEqual(collector.calls, 1)
        self.assertEqual(len(fixture.writer.calls), writer_calls_before)

    def test_writer_exception_is_sanitized(self) -> None:
        secret = "SECRET-RESUMABLE-WRITER/C:/private"
        fixture = Fixture()
        fixture.writer.raise_error = RuntimeError(secret)
        with self.assertRaises(QualityPilotResumableServiceError) as raised:
            fixture.service.run(fixture.genesis_request(), fixture.reader(), SequenceCollector(), fixture.writer)
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)


class ConcurrencyTests(unittest.TestCase):
    def test_second_successor_at_the_same_predecessor_and_spec_fails_closed(self) -> None:
        fixture = Fixture()
        request = fixture.genesis_request()

        branch_a = fixture.service.run(request, fixture.reader(), SequenceCollector(offset_seconds=1), fixture.writer)
        branch_a.verify_content_identity()

        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.service.run(request, fixture.reader(), SequenceCollector(offset_seconds=2), fixture.writer)

        stored_bytes, stored_generation = fixture.writer.store[
            (branch_a.published_transition.bucket, branch_a.published_transition.object_name)
        ]
        self.assertEqual(stored_generation, branch_a.published_transition.generation)


class MixedBranchAttackTests(unittest.TestCase):
    """Reproduces Codex's exact independently discovered attack: build two
    genuinely valid branches from the same genesis point with different
    observation timestamps, then attempt to mix one branch's request/run/
    snapshot with the other branch's transition record/publication."""

    def setUp(self) -> None:
        self.fixture_a = Fixture()
        self.fixture_b = Fixture()
        first_a = self.fixture_a.plan.capture_specs[0]
        first_b = self.fixture_b.plan.capture_specs[0]
        request_a = QualityPilotResumableCaptureRequest(
            plan_pin=self.fixture_a.plan_pin, predecessor_transition_pin=None,
            target_capture_spec_id=first_a.capture_spec_id, invocation_at=first_a.window.opens_at,
        )
        request_b = QualityPilotResumableCaptureRequest(
            plan_pin=self.fixture_b.plan_pin, predecessor_transition_pin=None,
            target_capture_spec_id=first_b.capture_spec_id, invocation_at=first_b.window.opens_at,
        )
        self.branch_a = self.fixture_a.service.run(
            request_a, self.fixture_a.reader(), SequenceCollector(offset_seconds=1), self.fixture_a.writer
        )
        self.branch_b = self.fixture_b.service.run(
            request_b, self.fixture_b.reader(), SequenceCollector(offset_seconds=2), self.fixture_b.writer
        )
        self.assertNotEqual(self.branch_a.run_result_id, self.branch_b.run_result_id)

    def test_mixing_run_snapshot_with_a_foreign_transition_is_rejected(self) -> None:
        a, b = self.branch_a, self.branch_b
        construct_failed = False
        mixed = None
        try:
            mixed = QualityPilotResumableCaptureResult(
                request_id=a.request_id, plan_id=a.plan_id, previous_snapshot_id=a.previous_snapshot_id,
                capture_spec_id=a.capture_spec_id, run_result_id=a.run_result_id, snapshot_id=a.snapshot_id,
                published_snapshot=a.published_snapshot,
                transition=b.transition, transition_id=b.transition_id, published_transition=b.published_transition,
            )
        except Exception:
            construct_failed = True
        verify_ok = False
        if mixed is not None:
            try:
                mixed.verify_content_identity()
                verify_ok = True
            except Exception:
                verify_ok = False
        self.assertTrue(construct_failed or not verify_ok)

    def test_every_pairwise_field_swap_is_rejected(self) -> None:
        a, b = self.branch_a, self.branch_b
        base_kwargs = dict(
            request_id=a.request_id, plan_id=a.plan_id, previous_snapshot_id=a.previous_snapshot_id,
            capture_spec_id=a.capture_spec_id, run_result_id=a.run_result_id, snapshot_id=a.snapshot_id,
            published_snapshot=a.published_snapshot,
            transition=a.transition, transition_id=a.transition_id, published_transition=a.published_transition,
        )
        # capture_spec_id/plan_id/request_id are identical across both
        # branches here (both branches genesis the same deterministic plan's
        # first spec), so swapping them is a no-op -- only fields that
        # actually differ between two genuinely distinct collector outputs
        # exercise the cross-field lineage checks.
        self.assertEqual(a.capture_spec_id, b.capture_spec_id)
        self.assertEqual(a.plan_id, b.plan_id)
        swaps = (
            {"run_result_id": b.run_result_id},
            {"snapshot_id": b.snapshot_id},
            {"published_snapshot": b.published_snapshot},
            {"transition": b.transition},
            {"transition_id": b.transition_id},
            {"published_transition": b.published_transition},
        )
        for swap in swaps:
            with self.subTest(swap=tuple(swap)):
                kwargs = dict(base_kwargs)
                kwargs.update(swap)
                construct_failed = False
                mixed = None
                try:
                    mixed = QualityPilotResumableCaptureResult(**kwargs)
                except Exception:
                    construct_failed = True
                verify_ok = False
                if mixed is not None:
                    try:
                        mixed.verify_content_identity()
                        verify_ok = True
                    except Exception:
                        verify_ok = False
                self.assertTrue(construct_failed or not verify_ok, f"swap {swap} was accepted")


class ResultTamperTests(unittest.TestCase):
    def _result(self) -> QualityPilotResumableCaptureResult:
        fixture = Fixture()
        return fixture.run_genesis()

    def test_tampering_request_id_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result, "request_id", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_plan_id_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result, "plan_id", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_capture_spec_id_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result, "capture_spec_id", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_run_result_id_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result, "run_result_id", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_snapshot_id_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result, "snapshot_id", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_published_snapshot_object_name_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result.published_snapshot, "object_name", "quality-pilot/v1/tampered.json")
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_transition_id_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result, "transition_id", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_transition_run_result_id_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result.transition, "run_result_id", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_published_transition_bucket_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result.published_transition, "bucket", "some-other-bucket")
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_service_result_id_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result, "service_result_id", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_posture_via_setattr_is_structurally_impossible(self) -> None:
        result = self._result()
        with self.assertRaises(AttributeError):
            object.__setattr__(result, "quality_only", False)

    def test_tampering_published_transition_byte_count_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result.published_transition, "encoded_byte_count", 1)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()

    def test_tampering_published_transition_hash_is_detected(self) -> None:
        result = self._result()
        object.__setattr__(result.published_transition, "encoded_sha256", "0" * 64)
        with self.assertRaises(QualityPilotResumableServiceError):
            result.verify_content_identity()


class ForgedTransitionPublicationTests(unittest.TestCase):
    """Reproduces Codex's exact independently discovered forgery: a valid
    PublishedQualityPilotLedgerTransition with all route/identity fields
    intact but a fabricated encoded_byte_count/encoded_sha256 that does not
    match the actual canonical bytes of the transition it claims to publish.
    """

    def _valid_result(self) -> QualityPilotResumableCaptureResult:
        fixture = Fixture()
        return fixture.run_genesis()

    def test_combined_byte_count_and_hash_forgery_is_rejected(self) -> None:
        result = self._valid_result()
        forged = replace(result.published_transition, encoded_byte_count=1, encoded_sha256="0" * 64)
        self._assert_forged_publication_rejected(result, forged)

    def test_byte_count_only_forgery_is_rejected(self) -> None:
        result = self._valid_result()
        forged = replace(result.published_transition, encoded_byte_count=result.published_transition.encoded_byte_count + 1)
        self._assert_forged_publication_rejected(result, forged)

    def test_hash_only_forgery_is_rejected(self) -> None:
        result = self._valid_result()
        forged = replace(result.published_transition, encoded_sha256="0" * 64)
        self._assert_forged_publication_rejected(result, forged)

    def _assert_forged_publication_rejected(self, result, forged_publication) -> None:
        construct_failed = False
        forged_result = None
        try:
            forged_result = QualityPilotResumableCaptureResult(
                request_id=result.request_id,
                plan_id=result.plan_id,
                previous_snapshot_id=result.previous_snapshot_id,
                capture_spec_id=result.capture_spec_id,
                run_result_id=result.run_result_id,
                snapshot_id=result.snapshot_id,
                published_snapshot=result.published_snapshot,
                transition=result.transition,
                transition_id=result.transition_id,
                published_transition=forged_publication,
            )
        except Exception:
            construct_failed = True
        verify_ok = False
        if forged_result is not None:
            try:
                forged_result.verify_content_identity()
                verify_ok = True
            except Exception:
                verify_ok = False
        self.assertTrue(construct_failed or not verify_ok, "forged transition publication was accepted")

    def test_malicious_writer_result_for_the_transition_publish_is_rejected(self) -> None:
        from india_swing.daily_pipeline.state_publication import PublishedStateObject

        fixture = Fixture()

        class SelectiveMaliciousWriter(SharedFakeWriter):
            """Behaves normally for the observation and snapshot writes, then
            returns a forged PublishedStateObject only for the transition
            write (identified by its distinctive object path)."""

            def create_or_verify(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
                if "/control/transitions/" in object_name:
                    return PublishedStateObject(
                        object_name=object_name, generation=1, byte_count=1, sha256="0" * 64
                    )
                return super().create_or_verify(
                    bucket=bucket, object_name=object_name, content_bytes=content_bytes,
                    content_type=content_type, maximum_bytes=maximum_bytes,
                )

        malicious_writer = SelectiveMaliciousWriter(fixture.reader_store)
        request = fixture.genesis_request()
        collector = SequenceCollector()
        with self.assertRaises(QualityPilotResumableServiceError):
            fixture.service.run(request, fixture.reader(), collector, malicious_writer)
        # The forged writer result is rejected before any result is returned;
        # only the (harmless, orphaned) observation/snapshot writes occurred.
        self.assertEqual(collector.calls, 1)


class AuditReplayTests(unittest.TestCase):
    def test_audit_reproduces_a_valid_chain(self) -> None:
        fixture = Fixture()
        final = fixture.advance_chain(10)
        key = (final.published_snapshot.bucket, final.published_snapshot.object_name, final.published_snapshot.generation)
        snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])
        prior_results = audit_replay_quality_pilot_completeness_snapshot(fixture.plan, snapshot, fixture.reader())
        self.assertEqual(len(prior_results), 10)

    def test_audit_detects_missing_observation(self) -> None:
        fixture = Fixture()
        final = fixture.advance_chain(5)
        key = (final.published_snapshot.bucket, final.published_snapshot.object_name, final.published_snapshot.generation)
        snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])
        pin = snapshot.pinned_observations[0]
        del fixture.reader_store[(pin.bucket, pin.object_name, pin.generation)]
        with self.assertRaises(QualityPilotResumableServiceError):
            audit_replay_quality_pilot_completeness_snapshot(fixture.plan, snapshot, fixture.reader())

    def test_audit_detects_reordered_observations(self) -> None:
        fixture = Fixture()
        final = fixture.advance_chain(5)
        key = (final.published_snapshot.bucket, final.published_snapshot.object_name, final.published_snapshot.generation)
        snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[key])
        reordered = replace(
            snapshot,
            completed_capture_spec_ids=(snapshot.completed_capture_spec_ids[1], snapshot.completed_capture_spec_ids[0])
            + snapshot.completed_capture_spec_ids[2:],
        )
        with self.assertRaises(QualityPilotResumableServiceError):
            audit_replay_quality_pilot_completeness_snapshot(fixture.plan, reordered, fixture.reader())

    def test_service_run_never_calls_audit_or_observation_replay(self) -> None:
        run_source = inspect.getsource(QualityPilotResumableCaptureService.run)
        self.assertNotIn("read_pinned_quality_pilot_observation", run_source)
        self.assertNotIn("audit_replay_quality_pilot_completeness_snapshot", run_source)
        self.assertNotIn("_reconstruct_prior_run_results", run_source)

    def test_ast_scan_of_run_method_contains_no_observation_replay_call(self) -> None:
        run_source = inspect.getsource(QualityPilotResumableCaptureService.run)
        tree = ast.parse(run_source.strip())
        called_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
        self.assertNotIn("read_pinned_quality_pilot_observation", called_names)
        self.assertNotIn("audit_replay_quality_pilot_completeness_snapshot", called_names)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_versions_are_pinned(self) -> None:
        self.assertEqual(
            QUALITY_PILOT_RESUMABLE_REQUEST_SCHEMA_VERSION, "quality_pilot_resumable_capture_request_v1"
        )
        self.assertEqual(
            QUALITY_PILOT_RESUMABLE_RESULT_SCHEMA_VERSION, "quality_pilot_resumable_capture_result_v1"
        )

    def test_module_has_no_clock_filesystem_network_or_trading_capability(self) -> None:
        source = inspect.getsource(service_module)
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
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_service_class_defines_no_scheduler_or_clock_helpers(self) -> None:
        source = inspect.getsource(service_module)
        tree = ast.parse(source)
        defined_names = {
            node.name.lower()
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        }
        self.assertEqual(defined_names & {"scheduler", "clock", "now", "sleep", "retry"}, set())

    def test_all_public_values_have_fixed_quality_only_posture(self) -> None:
        fixture = Fixture()
        result = fixture.run_genesis()
        for value in (fixture.genesis_request(), result):
            self.assertTrue(value.quality_only)
            for name in service_module._POSTURE_NAMES:
                self.assertEqual(getattr(value, name), name == "quality_only")


if __name__ == "__main__":
    unittest.main()
