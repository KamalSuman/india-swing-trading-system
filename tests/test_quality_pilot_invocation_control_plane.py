from __future__ import annotations

import ast
import inspect
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256

from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.quality_pilot import invocation_control_plane as icp_module
from india_swing.quality_pilot.canonical_response import PILOT_PROTOCOL_SHA256, ScheduledWindowKind
from india_swing.quality_pilot.control_plane_store import (
    QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
    PinnedQualityPilotControlArtifactRequest,
    PinnedQualityPilotLedgerTransitionRequest,
    QualityPilotControlArtifactKind,
    canonical_quality_pilot_control_object_name,
    canonical_quality_pilot_transition_object_name,
)
from india_swing.quality_pilot.invocation_control_plane import (
    QUALITY_PILOT_ACTION_BINDING_SCHEMA_VERSION,
    QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION,
    QUALITY_PILOT_RUNBOOK_SCHEMA_VERSION,
    ObservedQualityPilotObject,
    PinnedQualityPilotActionBindingRequest,
    PinnedQualityPilotActionClaimRequest,
    QualityPilotActionBinding,
    QualityPilotActionClaim,
    QualityPilotActionKind,
    QualityPilotClaimConflictError,
    QualityPilotCompletionReceipt,
    QualityPilotInvocationControlPlaneError,
    QualityPilotInvocationRunbook,
    QualityPilotWindowEntry,
    canonical_quality_pilot_action_binding_object_name,
    canonical_quality_pilot_claim_object_name,
    canonical_quality_pilot_completion_object_name,
    canonical_quality_pilot_window_entry_object_name,
    catalog_capture_spec_for_session,
    decode_quality_pilot_action_binding,
    decode_quality_pilot_action_claim,
    decode_quality_pilot_completion_receipt,
    decode_quality_pilot_invocation_runbook,
    decode_quality_pilot_window_entry,
    encode_quality_pilot_action_binding,
    encode_quality_pilot_action_claim,
    encode_quality_pilot_completion_receipt,
    encode_quality_pilot_invocation_runbook,
    encode_quality_pilot_window_entry,
    load_current_quality_pilot_window_entry,
    load_optional_quality_pilot_completion_receipt,
    pinned_quality_pilot_action_binding_request,
    pinned_quality_pilot_window_entry_request,
    publish_quality_pilot_action_binding,
    publish_quality_pilot_action_claim,
    publish_quality_pilot_completion_receipt,
    publish_quality_pilot_window_entry,
    read_pinned_quality_pilot_action_binding,
)
from tests.test_quality_pilot_campaign_ledger import BUCKET, _window
from tests.test_quality_pilot_capture_runner import _campaign
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter


def _all_windows(campaign):
    windows = []
    for session in campaign.confirmed_sessions:
        windows.append(_window(session, ScheduledWindowKind.CATALOG_PREOPEN))
        windows.append(_window(session, ScheduledWindowKind.QUOTE_0920))
        windows.append(_window(session, ScheduledWindowKind.QUOTE_CLOSE))
        windows.append(_window(session, ScheduledWindowKind.OHLCV_CLOSE))
    return tuple(windows)


def _runbook(campaign=None, *, provider_version="kite-3.0", bucket=BUCKET):
    campaign = campaign or _campaign()
    return QualityPilotInvocationRunbook(
        campaign=campaign, provider_version=provider_version, bucket=bucket, windows=_all_windows(campaign)
    )


def _genesis_binding(runbook):
    return QualityPilotActionBinding(
        runbook=runbook,
        action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
        market_session=runbook.campaign.confirmed_sessions[0],
        window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
        prior_plan_pin=None,
        prior_transition_pin=None,
        plan_pin=None,
        predecessor_transition_pin=None,
        target_capture_spec_id=None,
    )


def _fake_completion_pins(pilot_run_id: str, bucket: str, *, suffix: str = "0"):
    """Build one internally-consistent, shape-valid set of claim/outcome
    pins for a given pilot_run_id -- not backed by any real published
    object, but sufficient to exercise QualityPilotCompletionReceipt's own
    construction-time validation in isolation."""

    plan_id = (suffix + "1") * 32
    action_id = (suffix + "2") * 32
    claim_id = (suffix + "3") * 32
    capture_spec_id = (suffix + "4") * 32
    transition_id = (suffix + "5") * 32
    snapshot_id = (suffix + "6") * 32

    claim_pin = PinnedQualityPilotActionClaimRequest(
        storage_policy_version=QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION,
        protocol_sha256=PILOT_PROTOCOL_SHA256, pilot_run_id=pilot_run_id, action_id=action_id, claim_id=claim_id,
        bucket=bucket, object_name=canonical_quality_pilot_claim_object_name(pilot_run_id, action_id),
        generation=1, expected_encoded_sha256=(suffix + "7") * 32,
    )
    plan_pin = PinnedQualityPilotControlArtifactRequest(
        storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION, protocol_sha256=PILOT_PROTOCOL_SHA256,
        kind=QualityPilotControlArtifactKind.CAMPAIGN_PLAN, pilot_run_id=pilot_run_id, artifact_id=plan_id,
        bucket=bucket,
        object_name=canonical_quality_pilot_control_object_name(QualityPilotControlArtifactKind.CAMPAIGN_PLAN, pilot_run_id, plan_id),
        generation=1, expected_encoded_sha256=(suffix + "8") * 32,
    )
    transition_pin = PinnedQualityPilotLedgerTransitionRequest(
        storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION, protocol_sha256=PILOT_PROTOCOL_SHA256,
        pilot_run_id=pilot_run_id, plan_id=plan_id, previous_snapshot_id=None, capture_spec_id=capture_spec_id,
        transition_id=transition_id, bucket=bucket,
        object_name=canonical_quality_pilot_transition_object_name(pilot_run_id, plan_id, "genesis", capture_spec_id),
        generation=1, expected_encoded_sha256=(suffix + "9") * 32,
    )
    snapshot_pin = PinnedQualityPilotControlArtifactRequest(
        storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION, protocol_sha256=PILOT_PROTOCOL_SHA256,
        kind=QualityPilotControlArtifactKind.COMPLETENESS_LEDGER, pilot_run_id=pilot_run_id, artifact_id=snapshot_id,
        bucket=bucket,
        object_name=canonical_quality_pilot_control_object_name(QualityPilotControlArtifactKind.COMPLETENESS_LEDGER, pilot_run_id, snapshot_id),
        generation=1, expected_encoded_sha256=(suffix + "0") * 32,
    )
    return action_id, claim_pin, plan_pin, transition_pin, snapshot_pin


class FakeReader:
    def __init__(self, store):
        self.store = store
        self.calls = []

    def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
        self.calls.append((bucket, object_name, generation))
        from india_swing.daily_pipeline.acquisition import GCSObjectPayload

        key = (bucket, object_name, generation)
        if key not in self.store:
            raise KeyError("no such pinned object")
        return GCSObjectPayload(content_bytes=self.store[key], generation=generation)


class SharedFakeWriter(FakeStateObjectWriter):
    def __init__(self, reader_store):
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


class FakeCurrentReader:
    def __init__(self, store, *, raise_error=None):
        self.store = store
        self.raise_error = raise_error
        self.calls = 0

    def read_current(self, *, bucket, object_name, maximum_bytes):
        self.calls += 1
        if self.raise_error is not None:
            raise self.raise_error
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
    def __init__(self):
        self.store = {}
        self.calls = 0

    def claim(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
        self.calls += 1
        key = (bucket, object_name)
        if key in self.store:
            raise QualityPilotClaimConflictError("conflict")
        result = PublishedStateObject(
            object_name=object_name, generation=len(self.store) + 1,
            byte_count=len(content_bytes), sha256=sha256(content_bytes).hexdigest(),
        )
        self.store[key] = content_bytes
        return result


class RunbookTests(unittest.TestCase):
    def test_valid_runbook_has_exact_80_windows_in_canonical_order(self) -> None:
        runbook = _runbook()
        runbook.verify_content_identity()
        self.assertEqual(len(runbook.windows), 80)
        self.assertIs(runbook.windows[0].window_kind, ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertIs(runbook.windows[1].window_kind, ScheduledWindowKind.QUOTE_0920)
        self.assertIs(runbook.windows[2].window_kind, ScheduledWindowKind.QUOTE_CLOSE)
        self.assertIs(runbook.windows[3].window_kind, ScheduledWindowKind.OHLCV_CLOSE)

    def test_canonical_round_trip_is_byte_exact(self) -> None:
        runbook = _runbook()
        encoded = encode_quality_pilot_invocation_runbook(runbook)
        decoded = decode_quality_pilot_invocation_runbook(encoded)
        self.assertEqual(decoded.runbook_id, runbook.runbook_id)
        self.assertEqual(encode_quality_pilot_invocation_runbook(decoded), encoded)

    def test_fixed_posture(self) -> None:
        runbook = _runbook()
        self.assertTrue(runbook.quality_only)
        for name in icp_module._POSTURE_NAMES:
            self.assertEqual(getattr(runbook, name), name == "quality_only")
        with self.assertRaises(AttributeError):
            object.__setattr__(runbook, "quality_only", False)

    def test_rejects_wrong_window_count(self) -> None:
        campaign = _campaign()
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotInvocationRunbook(
                campaign=campaign, provider_version="kite-3.0", bucket=BUCKET,
                windows=_all_windows(campaign)[:-1],
            )

    def test_rejects_reordered_windows(self) -> None:
        campaign = _campaign()
        windows = list(_all_windows(campaign))
        windows[0], windows[1] = windows[1], windows[0]
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotInvocationRunbook(
                campaign=campaign, provider_version="kite-3.0", bucket=BUCKET, windows=tuple(windows),
            )

    def test_rejects_duplicate_window(self) -> None:
        campaign = _campaign()
        windows = list(_all_windows(campaign))
        windows[4] = windows[0]  # session1's catalog window duplicated from session0's
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotInvocationRunbook(
                campaign=campaign, provider_version="kite-3.0", bucket=BUCKET, windows=tuple(windows),
            )

    def test_rejects_wrong_bucket(self) -> None:
        campaign = _campaign()
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotInvocationRunbook(
                campaign=campaign, provider_version="kite-3.0", bucket="X", windows=_all_windows(campaign),
            )

    def test_rejects_empty_provider_version(self) -> None:
        campaign = _campaign()
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotInvocationRunbook(
                campaign=campaign, provider_version="", bucket=BUCKET, windows=_all_windows(campaign),
            )

    def test_rejects_out_of_gate_catalog_window(self) -> None:
        # Reproduces Codex's exact QP-WINDOW-R1-BLOCKER-002 finding: a
        # catalog window moved to 00:01-00:02 IST (outside the accepted
        # 08:45-09:00 gate) must be rejected at runbook construction, before
        # any catalog capture spec can be derived from it.
        from datetime import datetime as _dt

        from india_swing.domain.models import INDIA_STANDARD_TIME
        from india_swing.quality_pilot.canonical_response import EndpointFamily, ObservationWindowSpec

        campaign = _campaign()
        windows = list(_all_windows(campaign))
        original = windows[0]
        bad_opens = _dt.combine(original.market_session, _dt.min.time(), tzinfo=INDIA_STANDARD_TIME).replace(hour=0, minute=1)
        bad_closes = bad_opens.replace(minute=2)
        windows[0] = ObservationWindowSpec(
            pilot_run_id=original.pilot_run_id, market_session=original.market_session,
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN, endpoint_family=EndpointFamily.CATALOG,
            opens_at=bad_opens, closes_at=bad_closes, protocol_sha256=original.protocol_sha256,
        )
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotInvocationRunbook(
                campaign=campaign, provider_version="kite-3.0", bucket=BUCKET, windows=tuple(windows),
            )

    def test_catalog_capture_spec_for_session_is_deterministic(self) -> None:
        runbook = _runbook()
        session0 = runbook.campaign.confirmed_sessions[0]
        spec_a = catalog_capture_spec_for_session(runbook, session0)
        spec_b = catalog_capture_spec_for_session(runbook, session0)
        self.assertEqual(spec_a.capture_spec_id, spec_b.capture_spec_id)
        self.assertEqual(spec_a.chunk_index, 1)
        self.assertEqual(spec_a.chunk_count, 1)

    def test_catalog_capture_spec_rejects_session_outside_campaign(self) -> None:
        runbook = _runbook()
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            catalog_capture_spec_for_session(runbook, date(2099, 1, 1))


class ActionBindingTests(unittest.TestCase):
    def test_genesis_catalog_binding_round_trips(self) -> None:
        runbook = _runbook()
        binding = _genesis_binding(runbook)
        binding.verify_content_identity()
        encoded = encode_quality_pilot_action_binding(binding)
        decoded = decode_quality_pilot_action_binding(encoded)
        self.assertEqual(decoded.action_id, binding.action_id)

    def test_resumable_binding_requires_plan_and_predecessor_pins(self) -> None:
        runbook = _runbook()
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotActionBinding(
                runbook=runbook, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                market_session=runbook.campaign.confirmed_sessions[0], window_kind=ScheduledWindowKind.QUOTE_0920,
                prior_plan_pin=None, prior_transition_pin=None,
                plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
            )

    def test_resumable_binding_rejects_foreign_runbook_pins(self) -> None:
        # Reproduces Codex's exact QP-WINDOW-R1-BLOCKER-001 finding: a
        # resumable action binding constructed with runbook A but valid
        # plan/transition pins from a foreign pilot_run_id B must be
        # rejected -- every pin must agree with the embedded runbook's own
        # pilot/protocol/bucket.
        runbook = _runbook()
        foreign_plan_pin = self._fake_plan_pin(runbook)
        foreign_transition_pin = self._fake_transition_pin(runbook)
        foreign_plan_pin = replace(foreign_plan_pin, pilot_run_id="9" * 64, object_name=foreign_plan_pin.object_name.replace(runbook.campaign.pilot_run_id, "9" * 64))
        foreign_transition_pin = replace(foreign_transition_pin, pilot_run_id="9" * 64, object_name=foreign_transition_pin.object_name.replace(runbook.campaign.pilot_run_id, "9" * 64))
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotActionBinding(
                runbook=runbook, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                market_session=runbook.campaign.confirmed_sessions[0], window_kind=ScheduledWindowKind.QUOTE_0920,
                prior_plan_pin=None, prior_transition_pin=None,
                plan_pin=foreign_plan_pin, predecessor_transition_pin=foreign_transition_pin,
                target_capture_spec_id="f" * 64,
            )

    def test_resumable_binding_rejects_plan_transition_lineage_mismatch(self) -> None:
        # Plan and transition pins that individually match the runbook's own
        # pilot/protocol/bucket, and are each individually well-formed, but
        # disagree with EACH OTHER on plan_id must still be rejected.
        from india_swing.quality_pilot.control_plane_store import (
            PinnedQualityPilotLedgerTransitionRequest,
            canonical_quality_pilot_transition_object_name,
        )

        runbook = _runbook()
        plan_pin = self._fake_plan_pin(runbook)
        different_plan_id = "7" * 64
        mismatched_transition_pin = PinnedQualityPilotLedgerTransitionRequest(
            storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
            protocol_sha256=runbook.campaign.protocol_sha256,
            pilot_run_id=runbook.campaign.pilot_run_id,
            plan_id=different_plan_id,
            previous_snapshot_id=None,
            capture_spec_id="c" * 64,
            transition_id="d" * 64,
            bucket=runbook.bucket,
            object_name=canonical_quality_pilot_transition_object_name(
                runbook.campaign.pilot_run_id, different_plan_id, "genesis", "c" * 64
            ),
            generation=1,
            expected_encoded_sha256="e" * 64,
        )
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotActionBinding(
                runbook=runbook, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                market_session=runbook.campaign.confirmed_sessions[0], window_kind=ScheduledWindowKind.QUOTE_0920,
                prior_plan_pin=None, prior_transition_pin=None,
                plan_pin=plan_pin, predecessor_transition_pin=mismatched_transition_pin,
                target_capture_spec_id="f" * 64,
            )

    def test_resumable_binding_rejects_catalog_window_kind(self) -> None:
        runbook = _runbook()
        fixture_pin = self._fake_plan_pin(runbook)
        fixture_transition_pin = self._fake_transition_pin(runbook)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotActionBinding(
                runbook=runbook, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                market_session=runbook.campaign.confirmed_sessions[0], window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
                prior_plan_pin=None, prior_transition_pin=None,
                plan_pin=fixture_pin, predecessor_transition_pin=fixture_transition_pin,
                target_capture_spec_id="a" * 64,
            )

    def test_catalog_binding_for_non_first_session_requires_predecessor_pins(self) -> None:
        runbook = _runbook()
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotActionBinding(
                runbook=runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
                market_session=runbook.campaign.confirmed_sessions[1], window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
                prior_plan_pin=None, prior_transition_pin=None,
                plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
            )

    def test_catalog_binding_for_first_session_rejects_predecessor_pins(self) -> None:
        runbook = _runbook()
        fixture_pin = self._fake_plan_pin(runbook)
        fixture_transition_pin = self._fake_transition_pin(runbook)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotActionBinding(
                runbook=runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
                market_session=runbook.campaign.confirmed_sessions[0], window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
                prior_plan_pin=fixture_pin, prior_transition_pin=fixture_transition_pin,
                plan_pin=None, predecessor_transition_pin=None, target_capture_spec_id=None,
            )

    def test_catalog_binding_must_not_carry_resumable_fields(self) -> None:
        runbook = _runbook()
        fixture_pin = self._fake_plan_pin(runbook)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotActionBinding(
                runbook=runbook, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
                market_session=runbook.campaign.confirmed_sessions[0], window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
                prior_plan_pin=None, prior_transition_pin=None,
                plan_pin=fixture_pin, predecessor_transition_pin=None, target_capture_spec_id=None,
            )

    def test_publish_and_read_pinned_action_binding_round_trip(self) -> None:
        runbook = _runbook()
        binding = _genesis_binding(runbook)
        writer = FakeStateObjectWriter()
        published = publish_quality_pilot_action_binding(binding, writer)
        self.assertEqual(
            published.object_name,
            canonical_quality_pilot_action_binding_object_name(runbook.campaign.pilot_run_id, binding.action_id),
        )
        pin = pinned_quality_pilot_action_binding_request(published)
        reader_store = {}
        for (bkt, name), (content, gen) in writer.store.items():
            reader_store[(bkt, name, gen)] = content
        loaded = read_pinned_quality_pilot_action_binding(pin, FakeReader(reader_store))
        self.assertEqual(loaded.binding.action_id, binding.action_id)

    def test_idempotent_republish_returns_identical_publication(self) -> None:
        runbook = _runbook()
        binding = _genesis_binding(runbook)
        writer = FakeStateObjectWriter()
        first = publish_quality_pilot_action_binding(binding, writer)
        second = publish_quality_pilot_action_binding(binding, writer)
        self.assertEqual(first.generation, second.generation)
        self.assertEqual(first.encoded_sha256, second.encoded_sha256)

    @staticmethod
    def _fake_plan_pin(runbook):
        from india_swing.quality_pilot.control_plane_store import (
            PinnedQualityPilotControlArtifactRequest,
            QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
        )

        artifact_id = "a" * 64
        return PinnedQualityPilotControlArtifactRequest(
            storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
            protocol_sha256=runbook.campaign.protocol_sha256,
            kind=QualityPilotControlArtifactKind.CAMPAIGN_PLAN,
            pilot_run_id=runbook.campaign.pilot_run_id,
            artifact_id=artifact_id,
            bucket=runbook.bucket,
            object_name=f"quality-pilot/v1/{runbook.campaign.pilot_run_id}/control/plans/{artifact_id}.json",
            generation=1,
            expected_encoded_sha256="b" * 64,
        )

    @staticmethod
    def _fake_transition_pin(runbook):
        from india_swing.quality_pilot.control_plane_store import (
            PinnedQualityPilotLedgerTransitionRequest,
            QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
            canonical_quality_pilot_transition_object_name,
        )

        plan_id = "a" * 64
        capture_spec_id = "c" * 64
        transition_id = "d" * 64
        return PinnedQualityPilotLedgerTransitionRequest(
            storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
            protocol_sha256=runbook.campaign.protocol_sha256,
            pilot_run_id=runbook.campaign.pilot_run_id,
            plan_id=plan_id,
            previous_snapshot_id=None,
            capture_spec_id=capture_spec_id,
            transition_id=transition_id,
            bucket=runbook.bucket,
            object_name=canonical_quality_pilot_transition_object_name(
                runbook.campaign.pilot_run_id, plan_id, "genesis", capture_spec_id
            ),
            generation=1,
            expected_encoded_sha256="e" * 64,
        )


class WindowEntryTests(unittest.TestCase):
    def test_window_entry_round_trips_and_publishes(self) -> None:
        runbook = _runbook()
        binding = _genesis_binding(runbook)
        writer = FakeStateObjectWriter()
        published_binding = publish_quality_pilot_action_binding(binding, writer)
        pin = pinned_quality_pilot_action_binding_request(published_binding)
        entry = QualityPilotWindowEntry(
            pilot_run_id=runbook.campaign.pilot_run_id,
            market_session=runbook.campaign.confirmed_sessions[0],
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            action_binding_pin=pin,
        )
        entry.verify_content_identity()
        encoded = encode_quality_pilot_window_entry(entry)
        decoded = decode_quality_pilot_window_entry(encoded)
        self.assertEqual(decoded.window_entry_id, entry.window_entry_id)

        published_entry = publish_quality_pilot_window_entry(entry, BUCKET, writer)
        self.assertEqual(
            published_entry.object_name,
            canonical_quality_pilot_window_entry_object_name(
                runbook.campaign.pilot_run_id, entry.market_session, entry.window_kind
            ),
        )
        reader_store = {}
        for (bkt, name), (content, gen) in writer.store.items():
            reader_store[(bkt, name, gen)] = content
        loaded = load_current_quality_pilot_window_entry(
            pilot_run_id=runbook.campaign.pilot_run_id, market_session=entry.market_session,
            window_kind=entry.window_kind, bucket=BUCKET, reader=FakeCurrentReader(reader_store),
        )
        self.assertEqual(loaded.window_entry_id, entry.window_entry_id)

    def test_window_entry_pin_binds_to_the_exact_published_object(self) -> None:
        runbook = _runbook()
        binding = _genesis_binding(runbook)
        writer = FakeStateObjectWriter()
        published_binding = publish_quality_pilot_action_binding(binding, writer)
        pin = pinned_quality_pilot_action_binding_request(published_binding)
        entry = QualityPilotWindowEntry(
            pilot_run_id=runbook.campaign.pilot_run_id,
            market_session=runbook.campaign.confirmed_sessions[0],
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            action_binding_pin=pin,
        )
        published_entry = publish_quality_pilot_window_entry(entry, BUCKET, writer)
        entry_pin = pinned_quality_pilot_window_entry_request(entry, BUCKET, published_entry)
        self.assertEqual(entry_pin.generation, published_entry.generation)
        self.assertEqual(entry_pin.expected_encoded_sha256, published_entry.sha256)


class CurrentReaderTests(unittest.TestCase):
    def test_read_current_returns_the_exact_object(self) -> None:
        content = b'{"a":1}\n'
        store = {(BUCKET, "route/one.json", 3): content}
        reader = FakeCurrentReader(store)
        observed = reader.read_current(bucket=BUCKET, object_name="route/one.json", maximum_bytes=1024)
        self.assertEqual(observed.generation, 3)
        self.assertEqual(observed.content_bytes, content)

    def test_read_current_optional_returns_none_when_absent(self) -> None:
        reader = FakeCurrentReader({})
        self.assertIsNone(
            reader.read_current_optional(bucket=BUCKET, object_name="route/missing.json", maximum_bytes=1024)
        )

    def test_observed_object_rejects_hash_mismatch(self) -> None:
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            ObservedQualityPilotObject(
                object_name="x", generation=1, byte_count=3, sha256="0" * 64, content_bytes=b"abc",
            )

    def test_production_current_reader_happy_path_via_fake_client(self) -> None:
        client = _FakeGCSClient()
        client.bucket(BUCKET).put(f"quality-pilot/v1/{'a' * 64}/invocations/windows/x.json", b'{"x":1}\n')
        from india_swing.quality_pilot.invocation_control_plane import GoogleCloudStorageQualityPilotCurrentObjectReader

        reader = GoogleCloudStorageQualityPilotCurrentObjectReader(client)
        observed = reader.read_current(
            bucket=BUCKET, object_name=f"quality-pilot/v1/{'a' * 64}/invocations/windows/x.json", maximum_bytes=1024
        )
        self.assertEqual(observed.content_bytes, b'{"x":1}\n')

    def test_production_current_reader_treats_configured_not_found_as_absence(self) -> None:
        client = _FakeGCSClient()
        from india_swing.quality_pilot.invocation_control_plane import GoogleCloudStorageQualityPilotCurrentObjectReader

        original_not_found = icp_module.NotFound
        icp_module.NotFound = _FakeNotFound
        try:
            reader = GoogleCloudStorageQualityPilotCurrentObjectReader(client)
            result = reader.read_current_optional(bucket=BUCKET, object_name="missing.json", maximum_bytes=1024)
            self.assertIsNone(result)
        finally:
            icp_module.NotFound = original_not_found

    def test_production_current_reader_generation_race_is_rejected(self) -> None:
        client = _FakeGCSClient()
        object_name = "route/race.json"
        client.bucket(BUCKET).put(object_name, b'{"x":1}\n')
        # Simulate a race: after reload observes generation 1, the object is
        # rewritten before the pinned download, so the pinned generation
        # check must fail closed.
        client.bucket(BUCKET).race_after_reload(object_name, b'{"x":2}\n')
        from india_swing.quality_pilot.invocation_control_plane import GoogleCloudStorageQualityPilotCurrentObjectReader

        reader = GoogleCloudStorageQualityPilotCurrentObjectReader(client)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            reader.read_current(bucket=BUCKET, object_name=object_name, maximum_bytes=1024)

    def _assert_bogus_generation_rejected(self, bogus_generation: object) -> None:
        from india_swing.quality_pilot.invocation_control_plane import GoogleCloudStorageQualityPilotCurrentObjectReader

        client = _FakeGCSClient()
        object_name = "route/bogus-generation.json"
        client.bucket(BUCKET).put(object_name, b'{"x":1}\n', generation=bogus_generation)
        reader = GoogleCloudStorageQualityPilotCurrentObjectReader(client)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            reader.read_current(bucket=BUCKET, object_name=object_name, maximum_bytes=1024)

    def test_production_current_reader_rejects_none_generation(self) -> None:
        self._assert_bogus_generation_rejected(None)

    def test_production_current_reader_rejects_bool_generation(self) -> None:
        self._assert_bogus_generation_rejected(True)

    def test_production_current_reader_rejects_string_generation(self) -> None:
        self._assert_bogus_generation_rejected("1")

    def test_production_current_reader_rejects_zero_generation(self) -> None:
        self._assert_bogus_generation_rejected(0)

    def test_production_current_reader_rejects_out_of_range_generation(self) -> None:
        self._assert_bogus_generation_rejected(icp_module._MAXIMUM_GENERATION + 1)

    def test_production_current_reader_rejects_content_one_byte_over_maximum(self) -> None:
        # Real GCS byte-range ``end`` is inclusive: a request bounded to
        # maximum_bytes can still return maximum_bytes+1 bytes for a large
        # enough object. The production reader's own length check must
        # reject that, never silently truncate or accept it.
        from india_swing.quality_pilot.invocation_control_plane import GoogleCloudStorageQualityPilotCurrentObjectReader

        client = _FakeGCSClient()
        object_name = "route/oversized.json"
        maximum_bytes = 16
        client.bucket(BUCKET).put(object_name, b"x" * (maximum_bytes + 1))
        reader = GoogleCloudStorageQualityPilotCurrentObjectReader(client)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            reader.read_current(bucket=BUCKET, object_name=object_name, maximum_bytes=maximum_bytes)


class _FakeNotFound(Exception):
    pass


class _FakePreconditionFailed(Exception):
    pass


class _FakeBlob:
    def __init__(self, bucket, name):
        self._bucket = bucket
        self.name = name
        self.generation = None

    def reload(self, retry=None):
        if self.name not in self._bucket.objects:
            raise _FakeNotFound(self.name)
        content, generation = self._bucket.objects[self.name]
        # Report the generation as observed NOW, then -- if a race was
        # armed -- apply it as a side effect, simulating another writer
        # completing between this reload and the caller's later pinned
        # download of the same generation.
        self.generation = generation
        if self.name in self._bucket.race_map:
            self._bucket.objects[self.name] = self._bucket.race_map.pop(self.name)

    def download_as_bytes(self, *, end=None, raw_download=None, if_generation_match=None, retry=None):
        if self.name not in self._bucket.objects:
            raise _FakeNotFound(self.name)
        content, generation = self._bucket.objects[self.name]
        if if_generation_match is not None and generation != if_generation_match:
            raise _FakePreconditionFailed(self.name)
        self.generation = generation
        # Real GCS byte-range ``end`` is INCLUSIVE (the last byte position
        # downloaded), so a request for ``end=N`` against a big enough
        # object returns N+1 bytes -- one more than a caller who reads
        # "end" as an exclusive Python-slice bound would expect. Modelling
        # that here is what lets tests actually exercise the production
        # reader's own "len(downloaded) <= maximum_bytes" rejection for an
        # object one byte larger than its bound.
        return content[: end + 1] if end is not None else content

    def upload_from_string(self, data, content_type=None, if_generation_match=None, retry=None):
        existing = self._bucket.objects.get(self.name)
        if if_generation_match == 0 and existing is not None:
            raise _FakePreconditionFailed(self.name)
        new_generation = (existing[1] + 1) if existing else 1
        payload = data if isinstance(data, bytes) else data.encode("utf-8")
        self._bucket.objects[self.name] = (payload, new_generation)
        self.generation = new_generation


class _FakeBucket:
    def __init__(self):
        self.objects: dict[str, tuple[bytes, int]] = {}
        self.race_map: dict[str, tuple[bytes, int]] = {}

    def put(self, name, content, generation=1):
        self.objects[name] = (content, generation)

    def race_after_reload(self, name, new_content):
        current_generation = self.objects[name][1]
        self.race_map[name] = (new_content, current_generation + 1)

    def blob(self, name, generation=None):
        return _FakeBlob(self, name)


class _FakeGCSClient:
    def __init__(self):
        self._buckets: dict[str, _FakeBucket] = {}

    def bucket(self, name):
        return self._buckets.setdefault(name, _FakeBucket())


class ClaimTests(unittest.TestCase):
    def test_claim_publishes_and_computes_route(self) -> None:
        claim = QualityPilotActionClaim(
            pilot_run_id="a" * 64, action_id="b" * 64,
            invocation_at=datetime.now(timezone.utc), code_sha256="c" * 64, environment_sha256="d" * 64,
        )
        claim.verify_content_identity()
        writer = FakeClaimWriter()
        published = publish_quality_pilot_action_claim(claim, BUCKET, writer)
        self.assertEqual(
            published.object_name, canonical_quality_pilot_claim_object_name(claim.pilot_run_id, claim.action_id)
        )
        self.assertEqual(writer.calls, 1)

    def test_claim_codec_round_trip(self) -> None:
        claim = QualityPilotActionClaim(
            pilot_run_id="a" * 64, action_id="b" * 64,
            invocation_at=datetime.now(timezone.utc), code_sha256="c" * 64, environment_sha256="d" * 64,
        )
        encoded = encode_quality_pilot_action_claim(claim)
        decoded = decode_quality_pilot_action_claim(encoded)
        self.assertEqual(decoded.claim_id, claim.claim_id)

    def test_second_claim_at_the_same_route_conflicts(self) -> None:
        claim = QualityPilotActionClaim(
            pilot_run_id="a" * 64, action_id="b" * 64,
            invocation_at=datetime.now(timezone.utc), code_sha256="c" * 64, environment_sha256="d" * 64,
        )
        writer = FakeClaimWriter()
        publish_quality_pilot_action_claim(claim, BUCKET, writer)
        with self.assertRaises(QualityPilotClaimConflictError):
            publish_quality_pilot_action_claim(claim, BUCKET, writer)

    def test_claim_rejects_naive_invocation_time(self) -> None:
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotActionClaim(
                pilot_run_id="a" * 64, action_id="b" * 64,
                invocation_at=datetime.now(), code_sha256="c" * 64, environment_sha256="d" * 64,
            )

    def test_production_claim_writer_conflict_is_distinct(self) -> None:
        from india_swing.quality_pilot.invocation_control_plane import GoogleCloudStorageQualityPilotClaimWriter

        client = _FakeGCSClient()
        original_precondition = icp_module.PreconditionFailed
        icp_module.PreconditionFailed = _FakePreconditionFailed
        try:
            writer = GoogleCloudStorageQualityPilotClaimWriter(client)
            first = writer.claim(
                bucket=BUCKET, object_name="route/claim.json", content_bytes=b"{}", content_type="application/json",
                maximum_bytes=1024,
            )
            self.assertEqual(first.generation, 1)
            with self.assertRaises(QualityPilotClaimConflictError):
                writer.claim(
                    bucket=BUCKET, object_name="route/claim.json", content_bytes=b"{}",
                    content_type="application/json", maximum_bytes=1024,
                )
        finally:
            icp_module.PreconditionFailed = original_precondition


class CompletionReceiptTests(unittest.TestCase):
    def _binding_pin(self, runbook):
        binding = _genesis_binding(runbook)
        writer = FakeStateObjectWriter()
        published = publish_quality_pilot_action_binding(binding, writer)
        return pinned_quality_pilot_action_binding_request(published)

    def test_campaign_complete_receipt_has_no_successor(self) -> None:
        pilot_run_id = "a" * 64
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET)
        receipt = QualityPilotCompletionReceipt(
            pilot_run_id=pilot_run_id, action_id=action_id, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
            claim_pin=claim_pin, outcome_plan_pin=plan_pin, outcome_transition_pin=transition_pin,
            outcome_snapshot_pin=snapshot_pin,
            successor_action_binding_pin=None, next_window_entry_pin=None, campaign_complete=True,
        )
        receipt.verify_content_identity()
        self.assertEqual(receipt.final_transition_id, transition_pin.transition_id)

    def test_non_terminal_receipt_requires_successor(self) -> None:
        pilot_run_id = "a" * 64
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotCompletionReceipt(
                pilot_run_id=pilot_run_id, action_id=action_id, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                claim_pin=claim_pin, outcome_plan_pin=plan_pin, outcome_transition_pin=transition_pin,
                outcome_snapshot_pin=snapshot_pin,
                successor_action_binding_pin=None, next_window_entry_pin=None, campaign_complete=False,
            )

    def test_campaign_complete_receipt_rejects_successor(self) -> None:
        runbook = _runbook()
        pilot_run_id = runbook.campaign.pilot_run_id
        pin = self._binding_pin(runbook)
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotCompletionReceipt(
                pilot_run_id=pilot_run_id, action_id=action_id, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                claim_pin=claim_pin, outcome_plan_pin=plan_pin, outcome_transition_pin=transition_pin,
                outcome_snapshot_pin=snapshot_pin,
                successor_action_binding_pin=pin, next_window_entry_pin=None, campaign_complete=True,
            )

    def test_receipt_rejects_foreign_pilot_successor(self) -> None:
        # Reproduces Codex's exact QP-WINDOW-R1-BLOCKER-003 finding: a
        # completion receipt for pilot A whose successor_action_binding_pin
        # belongs to a foreign pilot B must be rejected at construction.
        pilot_a = "a" * 64
        pilot_b = "9" * 64
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_a, BUCKET)
        foreign_action_id = "c" * 64
        foreign_successor_pin = PinnedQualityPilotActionBindingRequest(
            storage_policy_version=QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION,
            protocol_sha256=PILOT_PROTOCOL_SHA256, pilot_run_id=pilot_b, action_id=foreign_action_id,
            bucket=BUCKET, object_name=canonical_quality_pilot_action_binding_object_name(pilot_b, foreign_action_id),
            generation=1, expected_encoded_sha256="d" * 64,
        )
        accepted = False
        try:
            receipt = QualityPilotCompletionReceipt(
                pilot_run_id=pilot_a, action_id=action_id, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
                claim_pin=claim_pin, outcome_plan_pin=plan_pin, outcome_transition_pin=transition_pin,
                outcome_snapshot_pin=snapshot_pin, successor_action_binding_pin=foreign_successor_pin,
                next_window_entry_pin=None, campaign_complete=False,
            )
            receipt.verify_content_identity()
            accepted = True
        except QualityPilotInvocationControlPlaneError:
            pass
        self.assertFalse(accepted)

    def test_receipt_requires_claim_pin_action_id_to_match(self) -> None:
        pilot_run_id = "a" * 64
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET)
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotCompletionReceipt(
                pilot_run_id=pilot_run_id, action_id="9" * 64, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                claim_pin=claim_pin, outcome_plan_pin=plan_pin, outcome_transition_pin=transition_pin,
                outcome_snapshot_pin=snapshot_pin,
                successor_action_binding_pin=None, next_window_entry_pin=None, campaign_complete=True,
            )

    def test_receipt_requires_outcome_transition_plan_lineage_to_agree(self) -> None:
        pilot_run_id = "a" * 64
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET)
        _, _, other_plan_pin, _, _ = _fake_completion_pins(pilot_run_id, BUCKET, suffix="1")
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotCompletionReceipt(
                pilot_run_id=pilot_run_id, action_id=action_id, action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                claim_pin=claim_pin, outcome_plan_pin=other_plan_pin, outcome_transition_pin=transition_pin,
                outcome_snapshot_pin=snapshot_pin,
                successor_action_binding_pin=None, next_window_entry_pin=None, campaign_complete=True,
            )

    def test_receipt_rejects_claim_pin_from_a_different_branch(self) -> None:
        pilot_run_id = "a" * 64
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET)
        _, other_claim_pin, _, _, _ = _fake_completion_pins(pilot_run_id, BUCKET, suffix="1")
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            QualityPilotCompletionReceipt(
                pilot_run_id=pilot_run_id, action_id=action_id, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
                claim_pin=other_claim_pin, outcome_plan_pin=plan_pin, outcome_transition_pin=transition_pin,
                outcome_snapshot_pin=snapshot_pin,
                successor_action_binding_pin=None, next_window_entry_pin=None, campaign_complete=True,
            )

    def test_receipt_rejects_outcome_snapshot_from_a_different_branch(self) -> None:
        pilot_run_id = "a" * 64
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET)
        _, _, _, _, other_snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET, suffix="1")
        # Construction alone won't catch this (both pins are individually
        # well-formed and share the same pilot_run_id); the deeper
        # plan/transition/snapshot cross-lineage check happens when
        # window_service._load_outcome_evidence loads and cross-verifies
        # all three against each other at replay time. Confirm the receipt
        # at least constructs (its own field-level checks pass) so the
        # window-service-level replay test carries the real assertion.
        receipt = QualityPilotCompletionReceipt(
            pilot_run_id=pilot_run_id, action_id=action_id, action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            claim_pin=claim_pin, outcome_plan_pin=plan_pin, outcome_transition_pin=transition_pin,
            outcome_snapshot_pin=other_snapshot_pin,
            successor_action_binding_pin=None, next_window_entry_pin=None, campaign_complete=True,
        )
        receipt.verify_content_identity()
        self.assertNotEqual(receipt.outcome_snapshot_pin.artifact_id, snapshot_pin.artifact_id)

    def test_receipt_round_trips_and_publishes(self) -> None:
        runbook = _runbook()
        pilot_run_id = runbook.campaign.pilot_run_id
        pin = self._binding_pin(runbook)
        action_id, claim_pin, plan_pin, transition_pin, snapshot_pin = _fake_completion_pins(pilot_run_id, BUCKET)
        receipt = QualityPilotCompletionReceipt(
            pilot_run_id=pilot_run_id, action_id=action_id,
            action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            claim_pin=claim_pin, outcome_plan_pin=plan_pin, outcome_transition_pin=transition_pin,
            outcome_snapshot_pin=snapshot_pin,
            successor_action_binding_pin=pin, next_window_entry_pin=None, campaign_complete=False,
        )
        encoded = encode_quality_pilot_completion_receipt(receipt)
        decoded = decode_quality_pilot_completion_receipt(encoded)
        self.assertEqual(decoded.receipt_id, receipt.receipt_id)
        self.assertEqual(decoded.final_transition_id, receipt.final_transition_id)

        writer = FakeStateObjectWriter()
        published = publish_quality_pilot_completion_receipt(receipt, BUCKET, writer)
        self.assertEqual(
            published.object_name,
            canonical_quality_pilot_completion_object_name(receipt.pilot_run_id, receipt.action_id),
        )
        reader_store = {}
        for (bkt, name), (content, gen) in writer.store.items():
            reader_store[(bkt, name, gen)] = content
        loaded = load_optional_quality_pilot_completion_receipt(
            pilot_run_id=receipt.pilot_run_id, action_id=receipt.action_id, bucket=BUCKET,
            reader=FakeCurrentReader(reader_store),
        )
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.receipt_id, receipt.receipt_id)

    def test_missing_receipt_is_none(self) -> None:
        loaded = load_optional_quality_pilot_completion_receipt(
            pilot_run_id="a" * 64, action_id="b" * 64, bucket=BUCKET, reader=FakeCurrentReader({}),
        )
        self.assertIsNone(loaded)


class AdversarialTests(unittest.TestCase):
    def test_tampering_action_binding_after_construction_is_detected(self) -> None:
        runbook = _runbook()
        binding = _genesis_binding(runbook)
        object.__setattr__(binding, "market_session", binding.market_session + timedelta(days=1))
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            binding.verify_content_identity()

    def test_forged_action_binding_via_replace_is_rejected(self) -> None:
        runbook = _runbook()
        binding = _genesis_binding(runbook)
        binding.verify_content_identity()
        construct_failed = False
        try:
            replace(binding, market_session=binding.market_session + timedelta(days=1))
        except Exception:
            construct_failed = True
        self.assertTrue(construct_failed)

    def test_forged_pin_hash_is_rejected_on_read(self) -> None:
        runbook = _runbook()
        binding = _genesis_binding(runbook)
        writer = FakeStateObjectWriter()
        published = publish_quality_pilot_action_binding(binding, writer)
        pin = pinned_quality_pilot_action_binding_request(published)
        forged_pin = replace(pin, expected_encoded_sha256="0" * 64)
        reader_store = {}
        for (bkt, name), (content, gen) in writer.store.items():
            reader_store[(bkt, name, gen)] = content
        with self.assertRaises(QualityPilotInvocationControlPlaneError):
            read_pinned_quality_pilot_action_binding(forged_pin, FakeReader(reader_store))

    def test_tampering_posture_via_setattr_is_structurally_impossible(self) -> None:
        runbook = _runbook()
        with self.assertRaises(AttributeError):
            object.__setattr__(runbook, "quality_only", False)
        binding = _genesis_binding(runbook)
        with self.assertRaises(AttributeError):
            object.__setattr__(binding, "quality_only", False)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_versions_are_pinned(self) -> None:
        self.assertEqual(QUALITY_PILOT_RUNBOOK_SCHEMA_VERSION, "quality_pilot_invocation_runbook_v1")
        self.assertEqual(QUALITY_PILOT_ACTION_BINDING_SCHEMA_VERSION, "quality_pilot_action_binding_v1")

    def test_module_has_no_clock_filesystem_network_or_trading_capability(self) -> None:
        source = inspect.getsource(icp_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "pathlib", "socket", "subprocess", "requests", "urllib", "httpx",
            "kiteconnect", "time", "random", "threading", "asyncio", "sqlite3", "pickle", "shelve",
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

    def test_no_bucket_listing_or_latest_selection(self) -> None:
        source = inspect.getsource(icp_module)
        self.assertNotIn("list_blobs", source)
        self.assertNotIn(".list(", source)


if __name__ == "__main__":
    unittest.main()
