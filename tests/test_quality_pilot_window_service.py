from __future__ import annotations

import ast
import inspect
import unittest
from dataclasses import replace
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from hashlib import sha256

from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.market_data.models import DailyCandle, DailyCandleBatch, NseSessionFinality
from india_swing.quality_pilot import window_service as ws_module
from india_swing.quality_pilot.canonical_response import (
    MAXIMUM_QUOTE_REQUEST_KEYS,
    EndpointFamily,
    ResponseClassification,
    ScheduledWindowKind,
)
from india_swing.quality_pilot.capture_runner import QualityPilotCollectionResult
from india_swing.quality_pilot.invocation_control_plane import (
    ObservedQualityPilotObject,
    QualityPilotActionBinding,
    QualityPilotActionKind,
    QualityPilotClaimConflictError,
    QualityPilotInvocationRunbook,
    QualityPilotWindowEntry,
    canonical_quality_pilot_claim_object_name,
    encode_quality_pilot_completion_receipt,
    pinned_quality_pilot_action_binding_request,
    publish_quality_pilot_action_binding,
    publish_quality_pilot_window_entry,
    read_pinned_quality_pilot_action_binding,
)
from india_swing.quality_pilot.window_service import (
    QualityPilotIndeterminateClaimedActionError,
    QualityPilotWindowService,
    QualityPilotWindowServiceError,
)
from tests.test_quality_pilot_campaign_ledger import BUCKET, _window
from tests.test_quality_pilot_canonical_response import _instrument, _catalog_payload, _quote, _quote_payload
from tests.test_quality_pilot_capture_runner import _campaign
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter

CODE_SHA = "a" * 64
ENV_SHA = "b" * 64


def _all_windows(campaign):
    windows = []
    for session in campaign.confirmed_sessions:
        windows.append(_window(session, ScheduledWindowKind.CATALOG_PREOPEN))
        windows.append(_window(session, ScheduledWindowKind.QUOTE_0920))
        windows.append(_window(session, ScheduledWindowKind.QUOTE_CLOSE))
        windows.append(_window(session, ScheduledWindowKind.OHLCV_CLOSE))
    return tuple(windows)


class SharedFakeWriter(FakeStateObjectWriter):
    def __init__(self, reader_store: dict) -> None:
        super().__init__()
        self.reader_store = reader_store

    def create_or_verify(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
        result = super().create_or_verify(
            bucket=bucket, object_name=object_name, content_bytes=content_bytes,
            content_type=content_type, maximum_bytes=maximum_bytes,
        )
        stored_bytes, _ = self.store[(bucket, object_name)]
        self.reader_store[(bucket, object_name, result.generation)] = stored_bytes
        return result


class FakeReader:
    def __init__(self, store: dict) -> None:
        self.store = store

    def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
        from india_swing.daily_pipeline.acquisition import GCSObjectPayload

        return GCSObjectPayload(content_bytes=self.store[(bucket, object_name, generation)], generation=generation)


class FakeCurrentReader:
    def __init__(self, store: dict) -> None:
        self.store = store

    def read_current(self, *, bucket, object_name, maximum_bytes):
        for (bkt, name, gen), content in self.store.items():
            if bkt == bucket and name == object_name:
                return ObservedQualityPilotObject(
                    object_name=object_name, generation=gen, byte_count=len(content),
                    sha256=sha256(content).hexdigest(), content_bytes=content,
                )
        raise KeyError("not found")

    def read_current_optional(self, *, bucket, object_name, maximum_bytes):
        try:
            return self.read_current(bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes)
        except KeyError:
            return None


class FakeClaimWriter:
    def __init__(self, reader_store: dict) -> None:
        self.store: dict = {}
        self.reader_store = reader_store
        self.calls = 0

    def claim(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
        self.calls += 1
        key = (bucket, object_name)
        if key in self.store:
            raise QualityPilotClaimConflictError("conflict")
        generation = len(self.store) + 1
        result = PublishedStateObject(
            object_name=object_name, generation=generation,
            byte_count=len(content_bytes), sha256=sha256(content_bytes).hexdigest(),
        )
        self.store[key] = content_bytes
        self.reader_store[(bucket, object_name, generation)] = content_bytes
        return result


def _payload_for_at(spec, start):
    if spec.window.endpoint_family is EndpointFamily.CATALOG:
        raise AssertionError("catalog capture is handled separately by the fixture")
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


class TrackingCollector:
    """Collector whose observation timestamp always tracks the last
    ``invocation_at`` the fixture's clock returned, so every capture stays
    causally consistent (observed_at >= invocation_at) regardless of how
    many actions the window service processes internally per call."""

    def __init__(self, clock_state: dict) -> None:
        self.calls = 0
        self._clock_state = clock_state

    def collect(self, spec):
        self.calls += 1
        last_returned = self._clock_state["last_returned"]
        start = max(spec.window.opens_at, last_returned) if last_returned is not None else spec.window.opens_at
        if spec.window.endpoint_family is EndpointFamily.CATALOG:
            payload = _catalog_payload(spec.window, self._clock_state["instruments"])
        else:
            payload = _payload_for_at(spec, start)
        return QualityPilotCollectionResult(
            request_started_at=start, request_ended_at=start + timedelta(seconds=1),
            response_classification=ResponseClassification.SUCCESS, payload=payload,
        )


class Fixture:
    def __init__(self, *, instrument_count: int = 3) -> None:
        self.campaign = _campaign()
        self.runbook = QualityPilotInvocationRunbook(
            campaign=self.campaign, provider_version="kite-3.0", bucket=BUCKET, windows=_all_windows(self.campaign),
        )
        self.reader_store: dict = {}
        self.writer = SharedFakeWriter(self.reader_store)
        self.claim_writer = FakeClaimWriter(self.reader_store)
        self.instruments = tuple(_instrument(token=100 + i, symbol=f"SYM{i:03d}") for i in range(instrument_count))
        self._clock_state = {"value": None, "last_returned": None, "instruments": self.instruments}
        self.collector = TrackingCollector(self._clock_state)
        self.service = QualityPilotWindowService()

    def set_clock_window(self, window) -> None:
        # Never move the clock backward: a second run_window() call for a
        # partially-processed window (restart/continuation) must keep
        # advancing from wherever the last invocation left off, matching a
        # real wall clock that never rewinds.
        current = self._clock_state["value"]
        if current is None or current < window.opens_at:
            self._clock_state["value"] = window.opens_at

    def clock(self) -> datetime:
        current = self._clock_state["value"]
        self._clock_state["last_returned"] = current
        self._clock_state["value"] = current + timedelta(seconds=1)
        return current

    def prepare_genesis(self) -> None:
        session0 = self.campaign.confirmed_sessions[0]
        binding = QualityPilotActionBinding(
            runbook=self.runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=session0, window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=None, prior_transition_pin=None,
            plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
        )
        published_binding = publish_quality_pilot_action_binding(binding, self.writer)
        pin = pinned_quality_pilot_action_binding_request(published_binding)
        entry = QualityPilotWindowEntry(
            pilot_run_id=self.campaign.pilot_run_id, market_session=session0,
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN, action_binding_pin=pin,
        )
        publish_quality_pilot_window_entry(entry, BUCKET, self.writer)

    def run_window(
        self, market_session, window_kind, *, maximum_actions_per_invocation: int = 500,
        code_sha256: str = CODE_SHA, environment_sha256: str = ENV_SHA,
    ):
        self.set_clock_window(_window(market_session, window_kind))
        return self.service.run(
            pilot_run_id=self.campaign.pilot_run_id, market_session=market_session, window_kind=window_kind,
            bucket=BUCKET, maximum_actions_per_invocation=maximum_actions_per_invocation,
            code_sha256=code_sha256, environment_sha256=environment_sha256, clock=self.clock,
            current_reader=FakeCurrentReader(self.reader_store), pinned_reader=FakeReader(self.reader_store),
            writer=self.writer, claim_writer=self.claim_writer, collector=self.collector,
        )

    def run_full_session(self, market_session):
        results = [
            self.run_window(market_session, ScheduledWindowKind.CATALOG_PREOPEN),
            self.run_window(market_session, ScheduledWindowKind.QUOTE_0920),
            self.run_window(market_session, ScheduledWindowKind.QUOTE_CLOSE),
            self.run_window(market_session, ScheduledWindowKind.OHLCV_CLOSE),
        ]
        return results


def _inject_transition(reader_store: dict, transition, *, generation: int):
    """Encode ``transition`` and place it directly at a caller-chosen
    (bucket, object_name, generation) slot in the shared fake reader store,
    bypassing ``publish_quality_pilot_ledger_transition``'s create-or-verify
    boundary entirely. This models exactly what a pinned reader trusts in
    production: whatever bytes sit at a caller-supplied generation -- the
    same trust boundary every other adversarial test in this suite probes
    by injecting/replacing bytes directly. Returns the matching pin."""
    from india_swing.quality_pilot.control_plane_store import (
        QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
        PinnedQualityPilotLedgerTransitionRequest,
        canonical_quality_pilot_transition_object_name,
        encode_quality_pilot_ledger_transition,
    )

    encoded = encode_quality_pilot_ledger_transition(transition)
    predecessor_token = "genesis" if transition.previous_snapshot_id is None else transition.previous_snapshot_id
    object_name = canonical_quality_pilot_transition_object_name(
        transition.pilot_run_id, transition.plan_id, predecessor_token, transition.capture_spec_id,
    )
    reader_store[(BUCKET, object_name, generation)] = encoded
    return PinnedQualityPilotLedgerTransitionRequest(
        storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
        protocol_sha256=transition.protocol_sha256, pilot_run_id=transition.pilot_run_id,
        plan_id=transition.plan_id, previous_snapshot_id=transition.previous_snapshot_id,
        capture_spec_id=transition.capture_spec_id, transition_id=transition.transition_id,
        bucket=BUCKET, object_name=object_name, generation=generation,
        expected_encoded_sha256=sha256(encoded).hexdigest(),
    )


def _inject_control_artifact(reader_store: dict, artifact, *, generation: int):
    """Same direct-injection technique as ``_inject_transition``, for a
    ``QualityPilotCampaignPlan``/``QualityPilotCompletenessSnapshot``.
    Returns the ``PublishedQualityPilotControlArtifact`` reference suitable
    for embedding directly as a transition's own ``next_snapshot``."""
    from india_swing.quality_pilot.control_plane_store import (
        QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
        PublishedQualityPilotControlArtifact,
        QualityPilotCampaignPlan,
        QualityPilotCompletenessSnapshot,
        QualityPilotControlArtifactKind,
        canonical_quality_pilot_control_object_name,
        encode_quality_pilot_campaign_plan,
        encode_quality_pilot_completeness_snapshot,
    )

    if type(artifact) is QualityPilotCampaignPlan:
        kind = QualityPilotControlArtifactKind.CAMPAIGN_PLAN
        artifact_id = artifact.plan_id
        encoded = encode_quality_pilot_campaign_plan(artifact)
    elif type(artifact) is QualityPilotCompletenessSnapshot:
        kind = QualityPilotControlArtifactKind.COMPLETENESS_LEDGER
        artifact_id = artifact.snapshot_id
        encoded = encode_quality_pilot_completeness_snapshot(artifact)
    else:
        raise AssertionError("unsupported control artifact type")
    object_name = canonical_quality_pilot_control_object_name(kind, artifact.pilot_run_id, artifact_id)
    reader_store[(BUCKET, object_name, generation)] = encoded
    return PublishedQualityPilotControlArtifact(
        storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION, protocol_sha256=artifact.protocol_sha256,
        kind=kind, pilot_run_id=artifact.pilot_run_id, artifact_id=artifact_id, bucket=BUCKET,
        object_name=object_name, generation=generation, encoded_byte_count=len(encoded),
        encoded_sha256=sha256(encoded).hexdigest(),
    )


def _read_control_artifact(fixture, published_ref):
    """Load the exact QualityPilotCampaignPlan/QualityPilotCompletenessSnapshot
    a PublishedQualityPilotControlArtifact reference (e.g. a transition's
    own ``next_snapshot``) resolves to."""
    from india_swing.quality_pilot.control_plane_store import (
        pinned_quality_pilot_control_artifact_request,
        read_pinned_quality_pilot_control_artifact,
    )

    pin = pinned_quality_pilot_control_artifact_request(published_ref)
    return read_pinned_quality_pilot_control_artifact(pin, FakeReader(fixture.reader_store)).artifact


def _inject_window_entry(fixture, entry, *, generation: int):
    """Same direct-injection technique as ``_inject_transition``, for a
    ``QualityPilotWindowEntry``."""
    from india_swing.quality_pilot.canonical_response import PILOT_PROTOCOL_SHA256
    from india_swing.quality_pilot.invocation_control_plane import (
        QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION,
        PinnedQualityPilotWindowEntryRequest,
        canonical_quality_pilot_window_entry_object_name,
        encode_quality_pilot_window_entry,
    )

    encoded = encode_quality_pilot_window_entry(entry)
    object_name = canonical_quality_pilot_window_entry_object_name(
        entry.pilot_run_id, entry.market_session, entry.window_kind,
    )
    fixture.reader_store[(BUCKET, object_name, generation)] = encoded
    return PinnedQualityPilotWindowEntryRequest(
        storage_policy_version=QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION, protocol_sha256=PILOT_PROTOCOL_SHA256,
        pilot_run_id=entry.pilot_run_id, market_session=entry.market_session, window_kind=entry.window_kind,
        bucket=BUCKET, object_name=object_name, generation=generation,
        expected_encoded_sha256=sha256(encoded).hexdigest(),
    )


def _session0_forensics(fixture):
    """Run session0 to completion and return the real artifacts needed to
    build revision-4 catalog-extension/replay forgeries: the genuine EARLY
    (catalog-only) transition, the genuine prior plan/transition pins the
    real session1 extension binding carries, and the genuine FINAL
    (fully-completed) transition those pins resolve to."""
    from india_swing.quality_pilot.invocation_control_plane import (
        canonical_quality_pilot_completion_object_name,
        decode_quality_pilot_completion_receipt,
        decode_quality_pilot_window_entry,
        read_pinned_quality_pilot_action_binding,
    )
    from india_swing.quality_pilot.control_plane_store import read_pinned_quality_pilot_ledger_transition

    session0 = fixture.campaign.confirmed_sessions[0]
    session1 = fixture.campaign.confirmed_sessions[1]
    fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
    catalog_binding = QualityPilotActionBinding(
        runbook=fixture.runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
        market_session=session0, window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
        prior_plan_pin=None, prior_transition_pin=None,
        plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
    )
    receipt_name0 = canonical_quality_pilot_completion_object_name(fixture.campaign.pilot_run_id, catalog_binding.action_id)
    key0 = next(k for k in fixture.reader_store if k[1] == receipt_name0)
    receipt0 = decode_quality_pilot_completion_receipt(fixture.reader_store[key0])
    early_transition = read_pinned_quality_pilot_ledger_transition(
        receipt0.outcome_transition_pin, FakeReader(fixture.reader_store)
    ).transition

    fixture.run_window(session0, ScheduledWindowKind.QUOTE_0920)
    fixture.run_window(session0, ScheduledWindowKind.QUOTE_CLOSE)
    fixture.run_window(session0, ScheduledWindowKind.OHLCV_CLOSE)

    entry1_key = next(
        k for k in fixture.reader_store if "/windows/" in k[1] and session1.isoformat() in k[1]
    )
    entry1 = decode_quality_pilot_window_entry(fixture.reader_store[entry1_key])
    loaded_session1_binding = read_pinned_quality_pilot_action_binding(
        entry1.action_binding_pin, FakeReader(fixture.reader_store)
    )
    real_prior_plan_pin = loaded_session1_binding.binding.prior_plan_pin
    real_prior_transition_pin = loaded_session1_binding.binding.prior_transition_pin
    real_final_transition = read_pinned_quality_pilot_ledger_transition(
        real_prior_transition_pin, FakeReader(fixture.reader_store)
    ).transition

    return {
        "receipt0": receipt0, "key0": key0, "catalog_binding": catalog_binding,
        "early_transition": early_transition,
        "real_prior_plan_pin": real_prior_plan_pin, "real_prior_transition_pin": real_prior_transition_pin,
        "real_final_transition": real_final_transition,
    }


class GenesisAndRestartTests(unittest.TestCase):
    def test_fresh_catalog_window_crosses_to_quote_0920(self) -> None:
        fixture = Fixture()
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        result = fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(result.actions_processed, 1)
        self.assertEqual(result.actions_reused, 0)
        self.assertFalse(result.campaign_complete)
        self.assertEqual(result.next_window_session, session0)
        self.assertIs(result.next_window_kind, ScheduledWindowKind.QUOTE_0920)
        self.assertEqual(fixture.collector.calls, 1)

    def test_restart_reuses_sealed_completion_with_zero_collector_calls(self) -> None:
        fixture = Fixture()
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        calls_before = fixture.collector.calls
        writes_before = len(fixture.writer.calls)
        result = fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(fixture.collector.calls, calls_before)
        self.assertEqual(len(fixture.writer.calls), writes_before)
        self.assertEqual(result.actions_reused, 1)

    def test_indeterminate_claim_fails_closed_with_zero_collector_calls(self) -> None:
        fixture = Fixture()
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]

        # Simulate a crashed prior process: the genesis action's claim
        # route already exists, but no completion was ever sealed.
        entry_key = None
        for key, content in fixture.reader_store.items():
            if "/actions/" in key[1]:
                entry_key = key
                break
        self.assertIsNotNone(entry_key)
        from india_swing.quality_pilot.invocation_control_plane import decode_quality_pilot_action_binding

        binding = decode_quality_pilot_action_binding(fixture.reader_store[entry_key])
        claim_object_name = canonical_quality_pilot_claim_object_name(fixture.campaign.pilot_run_id, binding.action_id)
        fixture.claim_writer.claim(
            bucket=BUCKET, object_name=claim_object_name, content_bytes=b'{"pre-existing":"claim"}',
            content_type="application/json", maximum_bytes=1024,
        )

        with self.assertRaises(QualityPilotIndeterminateClaimedActionError):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(fixture.collector.calls, 0)

    def test_maximum_actions_per_invocation_bounds_progress_without_erroring(self) -> None:
        # Reproduces Codex's exact QP-WINDOW-R1-BLOCKER-004 finding: a fresh
        # successful action must set an exact final_transition_id (never
        # None), and a maximum-actions stop before crossing must report
        # window_complete=False (WINDOW_PARTIAL), never a false "complete".
        fixture = Fixture(instrument_count=5)
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        genesis_result = fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertIsNotNone(genesis_result.final_transition_id)
        genesis_result.verify_content_identity()
        fixture.run_window(session0, ScheduledWindowKind.QUOTE_0920)
        fixture.run_window(session0, ScheduledWindowKind.QUOTE_CLOSE)
        # 5 OHLCV specs exist in this window; cap the invocation to 2.
        result = fixture.run_window(session0, ScheduledWindowKind.OHLCV_CLOSE, maximum_actions_per_invocation=2)
        self.assertEqual(result.actions_processed, 2)
        self.assertFalse(result.campaign_complete)
        self.assertFalse(result.window_complete)
        self.assertIsNotNone(result.final_transition_id)
        self.assertIsNone(result.next_window_session)
        self.assertIsNone(result.next_window_kind)
        result.verify_content_identity()
        # A follow-up invocation continues from where it left off.
        result2 = fixture.run_window(session0, ScheduledWindowKind.OHLCV_CLOSE, maximum_actions_per_invocation=10)
        self.assertEqual(result2.actions_processed, 5)
        self.assertEqual(result2.actions_reused, 2)
        self.assertTrue(result2.window_complete)


class MultiChunkAndCrossingTests(unittest.TestCase):
    def test_multi_chunk_quote_window_processes_every_chunk_in_order(self) -> None:
        fixture = Fixture(instrument_count=MAXIMUM_QUOTE_REQUEST_KEYS + 1)
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        result = fixture.run_window(session0, ScheduledWindowKind.QUOTE_0920)
        self.assertEqual(result.actions_processed, 2)  # 2 chunks: ceiling + 1
        self.assertIs(result.next_window_kind, ScheduledWindowKind.QUOTE_CLOSE)

    def test_ohlcv_window_bounded_ordered_processing_and_session_crossing(self) -> None:
        fixture = Fixture(instrument_count=3)
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        session1 = fixture.campaign.confirmed_sessions[1]
        fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        fixture.run_window(session0, ScheduledWindowKind.QUOTE_0920)
        fixture.run_window(session0, ScheduledWindowKind.QUOTE_CLOSE)
        result = fixture.run_window(session0, ScheduledWindowKind.OHLCV_CLOSE)
        self.assertEqual(result.actions_processed, 3)
        self.assertEqual(result.next_window_session, session1)
        self.assertIs(result.next_window_kind, ScheduledWindowKind.CATALOG_PREOPEN)

    def test_next_session_catalog_carries_exact_prior_pins(self) -> None:
        fixture = Fixture(instrument_count=3)
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        session1 = fixture.campaign.confirmed_sessions[1]
        fixture.run_full_session(session0)
        result = fixture.run_window(session1, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(result.actions_processed, 1)
        self.assertIs(result.next_window_kind, ScheduledWindowKind.QUOTE_0920)


class CampaignCompletionTests(unittest.TestCase):
    def test_full_campaign_reaches_completion_at_the_final_session(self) -> None:
        fixture = Fixture(instrument_count=2)
        fixture.prepare_genesis()
        sessions = fixture.campaign.confirmed_sessions
        campaign_complete = False
        for session in sessions:
            results = fixture.run_full_session(session)
            campaign_complete = results[-1].campaign_complete
            if campaign_complete:
                break
        self.assertTrue(campaign_complete)
        # Any further processing of the final session's OHLCV window must
        # reuse the sealed campaign-complete receipt with zero new calls.
        calls_before = fixture.collector.calls
        result = fixture.run_window(sessions[-1], ScheduledWindowKind.OHLCV_CLOSE)
        self.assertTrue(result.campaign_complete)
        self.assertEqual(fixture.collector.calls, calls_before)


class RejectionAndAdversarialTests(unittest.TestCase):
    def test_run_rejects_window_kind_mismatch_from_action_binding(self) -> None:
        fixture = Fixture()
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.run_window(session0, ScheduledWindowKind.QUOTE_0920)  # no window entry published yet

    def test_run_rejects_missing_window_entry(self) -> None:
        fixture = Fixture()
        session0 = fixture.campaign.confirmed_sessions[0]
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)

    def test_replay_rejects_outcome_snapshot_swapped_from_a_different_branch(self) -> None:
        # Two independently valid branches (different instrument counts, so
        # different plan/snapshot content even under the same deterministic
        # campaign): tamper branch A's sealed completion receipt so its
        # outcome_snapshot_pin points at branch B's real, validly-shaped,
        # correctly-hashed snapshot publication instead of branch A's own.
        # The coordinated swap must still be rejected at replay time,
        # because the swapped snapshot's own plan_id disagrees with branch
        # A's outcome plan.
        from dataclasses import replace as _replace

        from india_swing.quality_pilot.invocation_control_plane import (
            canonical_quality_pilot_completion_object_name,
            decode_quality_pilot_completion_receipt,
            encode_quality_pilot_completion_receipt,
        )

        fixture_a = Fixture(instrument_count=3)
        fixture_a.prepare_genesis()
        session0 = fixture_a.campaign.confirmed_sessions[0]
        fixture_a.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)

        fixture_b = Fixture(instrument_count=5)
        fixture_b.prepare_genesis()
        fixture_b.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)

        genesis_binding = QualityPilotActionBinding(
            runbook=fixture_a.runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=session0, window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=None, prior_transition_pin=None,
            plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
        )
        genesis_binding_b = QualityPilotActionBinding(
            runbook=fixture_b.runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=session0, window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=None, prior_transition_pin=None,
            plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
        )

        receipt_object_name_a = canonical_quality_pilot_completion_object_name(
            fixture_a.campaign.pilot_run_id, genesis_binding.action_id
        )
        receipt_object_name_b = canonical_quality_pilot_completion_object_name(
            fixture_b.campaign.pilot_run_id, genesis_binding_b.action_id
        )

        key_a = next(k for k in fixture_a.reader_store if k[1] == receipt_object_name_a)
        key_b = next(k for k in fixture_b.reader_store if k[1] == receipt_object_name_b)
        receipt_a = decode_quality_pilot_completion_receipt(fixture_a.reader_store[key_a])
        receipt_b = decode_quality_pilot_completion_receipt(fixture_b.reader_store[key_b])

        forged_receipt = _replace(receipt_a, outcome_snapshot_pin=receipt_b.outcome_snapshot_pin)
        construct_failed = False
        try:
            forged_bytes = encode_quality_pilot_completion_receipt(forged_receipt)
        except Exception:
            construct_failed = True
        if not construct_failed:
            fixture_a.reader_store[key_a] = forged_bytes

        replay_accepted = False
        try:
            fixture_a.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
            replay_accepted = True
        except QualityPilotWindowServiceError:
            pass
        self.assertTrue(construct_failed or not replay_accepted)

    def test_replay_rejects_different_code_digest(self) -> None:
        # Reproduces Codex's exact QP-WINDOW-R2-BLOCKER-005 finding: a
        # sealed completion's own claim carries code_sha256="a"*64; a later
        # invocation with a DIFFERENT code_sha256 must be rejected before
        # any collector call, never silently reused.
        fixture = Fixture()
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        calls_before = fixture.collector.calls
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN, code_sha256="9" * 64)
        self.assertEqual(fixture.collector.calls, calls_before)

    def test_replay_rejects_different_environment_digest(self) -> None:
        fixture = Fixture()
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        calls_before = fixture.collector.calls
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN, environment_sha256="9" * 64)
        self.assertEqual(fixture.collector.calls, calls_before)

    def test_fresh_catalog_extension_with_bogus_prior_lineage_never_reaches_collector(self) -> None:
        # Reproduces Codex's exact QP-WINDOW-R2-BLOCKER-006 finding: a
        # non-genesis catalog action with a validly-shaped but nonexistent
        # prior_plan_pin/prior_transition_pin must be rejected before its
        # claim is published or its collector is ever called.
        from india_swing.quality_pilot.canonical_response import PILOT_PROTOCOL_SHA256
        from india_swing.quality_pilot.control_plane_store import (
            QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
            PinnedQualityPilotControlArtifactRequest,
            PinnedQualityPilotLedgerTransitionRequest,
            QualityPilotControlArtifactKind,
            canonical_quality_pilot_control_object_name,
            canonical_quality_pilot_transition_object_name,
        )

        fixture = Fixture()
        fixture.prepare_genesis()
        session1 = fixture.campaign.confirmed_sessions[1]

        bogus_plan_id = "9" * 64
        bogus_plan_pin = PinnedQualityPilotControlArtifactRequest(
            storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION, protocol_sha256=PILOT_PROTOCOL_SHA256,
            kind=QualityPilotControlArtifactKind.CAMPAIGN_PLAN, pilot_run_id=fixture.campaign.pilot_run_id,
            artifact_id=bogus_plan_id, bucket=BUCKET,
            object_name=canonical_quality_pilot_control_object_name(QualityPilotControlArtifactKind.CAMPAIGN_PLAN, fixture.campaign.pilot_run_id, bogus_plan_id),
            generation=999, expected_encoded_sha256="8" * 64,
        )
        bogus_transition_pin = PinnedQualityPilotLedgerTransitionRequest(
            storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION, protocol_sha256=PILOT_PROTOCOL_SHA256,
            pilot_run_id=fixture.campaign.pilot_run_id, plan_id=bogus_plan_id, previous_snapshot_id=None,
            capture_spec_id="7" * 64, transition_id="6" * 64, bucket=BUCKET,
            object_name=canonical_quality_pilot_transition_object_name(fixture.campaign.pilot_run_id, bogus_plan_id, "genesis", "7" * 64),
            generation=999, expected_encoded_sha256="5" * 64,
        )
        bogus_binding = QualityPilotActionBinding(
            runbook=fixture.runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=session1, window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=bogus_plan_pin, prior_transition_pin=bogus_transition_pin,
            plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
        )
        published_bogus = publish_quality_pilot_action_binding(bogus_binding, fixture.writer)
        bogus_pin = pinned_quality_pilot_action_binding_request(published_bogus)
        bogus_entry = QualityPilotWindowEntry(
            pilot_run_id=fixture.campaign.pilot_run_id, market_session=session1,
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN, action_binding_pin=bogus_pin,
        )
        publish_quality_pilot_window_entry(bogus_entry, BUCKET, fixture.writer)

        calls_before = fixture.collector.calls
        claims_before = fixture.claim_writer.calls
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.run_window(session1, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(fixture.collector.calls, calls_before)
        self.assertEqual(fixture.claim_writer.calls, claims_before)

    def test_catalog_extension_predecessor_transition_not_final_capture_is_rejected(self) -> None:
        # Reproduces Codex's exact QP-WINDOW-R3-BLOCKER-007 finding: a
        # "coordinated earlier-transition/full-snapshot pairing" -- a real,
        # validly-shaped predecessor transition for the EARLY (catalog-only)
        # capture, whose next_snapshot is swapped for the REAL final,
        # fully-completed snapshot. Revision 3's pre-claim verifier only
        # checked plan_id agreement and snapshot completeness, never that
        # the transition itself resolves the plan's FINAL capture spec.
        # Tests the exact private call QualityPilotWindowService.run() makes
        # before any claim/collector call, since the deterministic window
        # entry/action-binding routes used for a full run_window() call are
        # already claimed by the real crossing this fixture performs.
        fixture = Fixture(instrument_count=1)
        fixture.prepare_genesis()
        forensics = _session0_forensics(fixture)
        early = forensics["early_transition"]
        final = forensics["real_final_transition"]

        forged = replace(early, next_snapshot=final.next_snapshot)
        forged_pin = _inject_transition(fixture.reader_store, forged, generation=999_999)

        session1 = fixture.campaign.confirmed_sessions[1]
        bogus_binding = QualityPilotActionBinding(
            runbook=fixture.runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=session1, window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=forensics["real_prior_plan_pin"], prior_transition_pin=forged_pin,
            plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
        )
        calls_before = fixture.collector.calls
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.service._verify_pre_claim_lineage(
                current_binding=bogus_binding, cached_reader=FakeReader(fixture.reader_store),
            )
        self.assertEqual(fixture.collector.calls, calls_before)

    def test_catalog_extension_predecessor_transition_run_result_mismatch_is_rejected(self) -> None:
        # The forged transition DOES resolve the plan's final capture spec
        # this time, but its run_result_id disagrees with the final
        # snapshot's own run_result_ids at that position.
        fixture = Fixture(instrument_count=1)
        fixture.prepare_genesis()
        forensics = _session0_forensics(fixture)
        final = forensics["real_final_transition"]

        forged = replace(final, run_result_id="f" * 64)
        forged_pin = _inject_transition(fixture.reader_store, forged, generation=999_999)

        session1 = fixture.campaign.confirmed_sessions[1]
        bogus_binding = QualityPilotActionBinding(
            runbook=fixture.runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=session1, window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=forensics["real_prior_plan_pin"], prior_transition_pin=forged_pin,
            plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
        )
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.service._verify_pre_claim_lineage(
                current_binding=bogus_binding, cached_reader=FakeReader(fixture.reader_store),
            )

    def test_catalog_extension_predecessor_snapshot_bucket_mismatch_is_rejected(self) -> None:
        fixture = Fixture(instrument_count=1)
        fixture.prepare_genesis()
        forensics = _session0_forensics(fixture)
        final = forensics["real_final_transition"]
        real_snapshot = _read_control_artifact(fixture, final.next_snapshot)

        # The snapshot's own _validate already requires every pinned
        # observation's bucket to equal the snapshot's bucket, so a bucket
        # forgery must move both together -- the revision-4 gap under test
        # is that NEITHER was ever compared against runbook.bucket.
        rebucketed_observations = tuple(
            replace(pin, bucket="a-different-quality-pilot-bucket") for pin in real_snapshot.pinned_observations
        )
        forged_snapshot = replace(
            real_snapshot, bucket="a-different-quality-pilot-bucket", pinned_observations=rebucketed_observations,
        )
        forged_next_snapshot = _inject_control_artifact(fixture.reader_store, forged_snapshot, generation=999_999)
        forged_transition = replace(final, next_snapshot=forged_next_snapshot)
        forged_transition_pin = _inject_transition(fixture.reader_store, forged_transition, generation=999_998)

        session1 = fixture.campaign.confirmed_sessions[1]
        bogus_binding = QualityPilotActionBinding(
            runbook=fixture.runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=session1, window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=forensics["real_prior_plan_pin"], prior_transition_pin=forged_transition_pin,
            plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
        )
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.service._verify_pre_claim_lineage(
                current_binding=bogus_binding, cached_reader=FakeReader(fixture.reader_store),
            )

    def test_replay_rejects_outcome_transition_run_result_mismatch(self) -> None:
        # Reproduces Codex's exact QP-WINDOW-R3-BLOCKER-008 finding:
        # _load_outcome_evidence never read transition.run_result_id at
        # all, so a forged transition whose run_result_id disagrees with
        # the outcome snapshot's own run_result_ids at the current capture
        # was accepted on replay.
        fixture = Fixture(instrument_count=1)
        fixture.prepare_genesis()
        forensics = _session0_forensics(fixture)
        receipt0 = forensics["receipt0"]
        real0 = forensics["early_transition"]

        forged = replace(real0, run_result_id="f" * 64)
        forged_pin = _inject_transition(fixture.reader_store, forged, generation=999_997)

        # Changing outcome_transition_pin also changes what the canonical
        # successor and next-window entry SHOULD point at (its own
        # predecessor_transition_pin); republish both consistently so the
        # ONLY remaining discrepancy is the run_result_id itself, not an
        # unrelated successor/entry mismatch.
        real_successor = read_pinned_quality_pilot_action_binding(
            receipt0.successor_action_binding_pin, FakeReader(fixture.reader_store)
        ).binding
        consistent_successor = replace(real_successor, predecessor_transition_pin=forged_pin)
        published_successor = publish_quality_pilot_action_binding(consistent_successor, fixture.writer)
        consistent_successor_pin = pinned_quality_pilot_action_binding_request(published_successor)

        session0 = fixture.campaign.confirmed_sessions[0]
        consistent_entry = QualityPilotWindowEntry(
            pilot_run_id=fixture.campaign.pilot_run_id, market_session=session0,
            window_kind=ScheduledWindowKind.QUOTE_0920, action_binding_pin=consistent_successor_pin,
        )
        consistent_entry_pin = _inject_window_entry(fixture, consistent_entry, generation=999_996)

        forged_receipt = replace(
            receipt0, outcome_transition_pin=forged_pin, successor_action_binding_pin=consistent_successor_pin,
            next_window_entry_pin=consistent_entry_pin,
        )
        fixture.reader_store[forensics["key0"]] = encode_quality_pilot_completion_receipt(forged_receipt)

        calls_before = fixture.collector.calls
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(fixture.collector.calls, calls_before)

    def test_replay_rejects_outcome_snapshot_observation_route_mismatch(self) -> None:
        # The outcome snapshot's pinned_observations[current_index] must
        # resolve to the CURRENT capture's exact route (session/window/
        # family/chunk/bucket) -- swap it for a shape-valid, self-consistent
        # pin naming a different window/family/chunk entirely (QUOTE_CLOSE/
        # FULL_QUOTE instead of the real CATALOG_PREOPEN/CATALOG capture).
        from dataclasses import replace as _dc_replace

        from india_swing.quality_pilot.observation_store import canonical_observation_object_name

        fixture = Fixture(instrument_count=1)
        fixture.prepare_genesis()
        forensics = _session0_forensics(fixture)
        receipt0 = forensics["receipt0"]
        real0 = forensics["early_transition"]
        real_snapshot = _read_control_artifact(fixture, real0.next_snapshot)
        real_pin = real_snapshot.pinned_observations[0]

        bogus_observation_pin = _dc_replace(
            real_pin, window_kind=ScheduledWindowKind.QUOTE_CLOSE, endpoint_family=EndpointFamily.FULL_QUOTE,
            chunk_index=1, chunk_count=1,
            object_name=canonical_observation_object_name(
                real_pin.pilot_run_id, real_pin.market_session, ScheduledWindowKind.QUOTE_CLOSE, 1, 1,
                real_pin.expected_observation_id,
            ),
        )
        forged_snapshot = replace(real_snapshot, pinned_observations=(bogus_observation_pin,))
        forged_next_snapshot = _inject_control_artifact(fixture.reader_store, forged_snapshot, generation=999_995)
        forged_transition = replace(real0, next_snapshot=forged_next_snapshot)
        forged_transition_pin = _inject_transition(fixture.reader_store, forged_transition, generation=999_994)

        real_successor = read_pinned_quality_pilot_action_binding(
            receipt0.successor_action_binding_pin, FakeReader(fixture.reader_store)
        ).binding
        consistent_successor = replace(real_successor, predecessor_transition_pin=forged_transition_pin)
        published_successor = publish_quality_pilot_action_binding(consistent_successor, fixture.writer)
        consistent_successor_pin = pinned_quality_pilot_action_binding_request(published_successor)

        session0 = fixture.campaign.confirmed_sessions[0]
        consistent_entry = QualityPilotWindowEntry(
            pilot_run_id=fixture.campaign.pilot_run_id, market_session=session0,
            window_kind=ScheduledWindowKind.QUOTE_0920, action_binding_pin=consistent_successor_pin,
        )
        consistent_entry_pin = _inject_window_entry(fixture, consistent_entry, generation=999_993)

        forged_receipt = replace(
            receipt0, outcome_transition_pin=forged_transition_pin,
            successor_action_binding_pin=consistent_successor_pin, next_window_entry_pin=consistent_entry_pin,
        )
        fixture.reader_store[forensics["key0"]] = encode_quality_pilot_completion_receipt(forged_receipt)

        calls_before = fixture.collector.calls
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(fixture.collector.calls, calls_before)

    def test_run_rejects_invalid_maximum_actions(self) -> None:
        fixture = Fixture()
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        with self.assertRaises(QualityPilotWindowServiceError):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN, maximum_actions_per_invocation=0)

    def test_result_rejects_reused_exceeding_processed(self) -> None:
        from india_swing.quality_pilot.window_service import QualityPilotWindowServiceResult

        with self.assertRaises(QualityPilotWindowServiceError):
            QualityPilotWindowServiceResult(
                pilot_run_id="a" * 64, market_session=_campaign().confirmed_sessions[0],
                window_kind=ScheduledWindowKind.CATALOG_PREOPEN, actions_processed=1, actions_reused=2,
                final_transition_id="c" * 64, campaign_complete=False, window_complete=True,
                next_window_session=_campaign().confirmed_sessions[0], next_window_kind=ScheduledWindowKind.QUOTE_0920,
            )

    def test_result_rejects_campaign_complete_with_next_window(self) -> None:
        from india_swing.quality_pilot.window_service import QualityPilotWindowServiceResult

        with self.assertRaises(QualityPilotWindowServiceError):
            QualityPilotWindowServiceResult(
                pilot_run_id="a" * 64, market_session=_campaign().confirmed_sessions[0],
                window_kind=ScheduledWindowKind.CATALOG_PREOPEN, actions_processed=1, actions_reused=0,
                final_transition_id="b" * 64, campaign_complete=True, window_complete=True,
                next_window_session=_campaign().confirmed_sessions[0], next_window_kind=ScheduledWindowKind.QUOTE_0920,
            )

    def test_result_rejects_window_complete_mismatch(self) -> None:
        from india_swing.quality_pilot.window_service import QualityPilotWindowServiceResult

        with self.assertRaises(QualityPilotWindowServiceError):
            QualityPilotWindowServiceResult(
                pilot_run_id="a" * 64, market_session=_campaign().confirmed_sessions[0],
                window_kind=ScheduledWindowKind.CATALOG_PREOPEN, actions_processed=1, actions_reused=0,
                final_transition_id="b" * 64, campaign_complete=False, window_complete=True,
                next_window_session=None, next_window_kind=None,
            )


class _StopBeforeWriter(SharedFakeWriter):
    """Delegates every write to the real fixture writer normally, except it
    raises BEFORE performing the first write whose object_name contains any
    of ``stop_before``. Everything written earlier in the same invocation
    has already durably landed in the shared reader store, exactly modelling
    a process crash right after a specific publication boundary."""

    def __init__(self, reader_store: dict, *, stop_before: tuple[str, ...]) -> None:
        super().__init__(reader_store)
        self.stop_before = stop_before

    def create_or_verify(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
        if any(token in object_name for token in self.stop_before):
            raise RuntimeError("simulated crash")
        return super().create_or_verify(
            bucket=bucket, object_name=object_name, content_bytes=content_bytes,
            content_type=content_type, maximum_bytes=maximum_bytes,
        )


class CrashBoundaryTests(unittest.TestCase):
    """A sealed completion replaying with zero writes is already covered by
    GenesisAndRestartTests.test_restart_reuses_sealed_completion_with_zero_collector_calls.
    These tests cover the three intermediate crash points within one
    fresh invocation: after the domain transition (plan/snapshot/transition)
    is durably published but before the successor binding; after the
    successor binding but before the next-window entry; and after the next-
    window entry but before the completion receipt. Every case must leave a
    RETRY indeterminate (claim already exists) with zero recollection --
    never a silent re-execution and never a false "already complete"."""

    def _crash_and_retry(self, stop_before: str) -> None:
        fixture = Fixture(instrument_count=1)
        fixture.prepare_genesis()
        session0 = fixture.campaign.confirmed_sessions[0]
        crashing_writer = _StopBeforeWriter(fixture.reader_store, stop_before=(stop_before,))
        fixture.writer = crashing_writer

        with self.assertRaises(Exception):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)

        # The claim from the crashed invocation is durably published (it is
        # always the very first thing published, before any domain call),
        # so a retry -- even with a working writer -- must remain
        # indeterminate rather than silently recollecting or re-publishing.
        fixture.writer = SharedFakeWriter(fixture.reader_store)
        calls_before = fixture.collector.calls
        with self.assertRaises(QualityPilotIndeterminateClaimedActionError):
            fixture.run_window(session0, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(fixture.collector.calls, calls_before)

    def test_crash_after_domain_transition_before_successor_binding_stays_indeterminate(self) -> None:
        # The successor action binding's own route is "/invocations/actions/";
        # stopping there lets the catalog capture + session-bootstrap's
        # plan/snapshot/transition writes complete first.
        self._crash_and_retry("/invocations/actions/")

    def test_crash_after_successor_binding_before_next_window_entry_stays_indeterminate(self) -> None:
        self._crash_and_retry("/invocations/windows/")

    def test_crash_after_next_window_entry_before_completion_receipt_stays_indeterminate(self) -> None:
        self._crash_and_retry("/invocations/completions/")


class CachingReaderTests(unittest.TestCase):
    def test_cache_key_includes_maximum_bytes(self) -> None:
        from india_swing.daily_pipeline.acquisition import GCSObjectPayload
        from india_swing.quality_pilot.window_service import _InvocationLocalCachingReader

        class CountingReader:
            def __init__(self) -> None:
                self.calls = 0

            def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
                self.calls += 1
                return GCSObjectPayload(content_bytes=b"x" * 10, generation=generation)

        underlying = CountingReader()
        cached = _InvocationLocalCachingReader(underlying)

        first = cached.read_generation(bucket=BUCKET, object_name="route/x.json", generation=1, maximum_bytes=1024)
        self.assertEqual(underlying.calls, 1)

        # Identical request (same maximum_bytes) is served from cache.
        second = cached.read_generation(bucket=BUCKET, object_name="route/x.json", generation=1, maximum_bytes=1024)
        self.assertEqual(underlying.calls, 1)
        self.assertIs(first, second)

        # A DIFFERENT maximum_bytes for the same (bucket, object_name,
        # generation) must never be served from a cache entry populated
        # under a different bound -- it must delegate to the reader again.
        cached.read_generation(bucket=BUCKET, object_name="route/x.json", generation=1, maximum_bytes=512)
        self.assertEqual(underlying.calls, 2)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_clock_filesystem_network_or_trading_capability(self) -> None:
        source = inspect.getsource(ws_module)
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
            "datetime.now(", "utcnow(", "getenv(", "os.environ", "sleep(", "list_blobs(",
            "place_order(", "generate_signal(", "run_paper_trade(", "cloud_run", "scheduler.",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_defines_no_scheduler_clock_or_collector_helpers(self) -> None:
        source = inspect.getsource(ws_module)
        tree = ast.parse(source)
        defined_names = {
            node.name.lower() for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        }
        self.assertEqual(defined_names & {"scheduler", "now", "sleep", "retry"}, set())


if __name__ == "__main__":
    unittest.main()
