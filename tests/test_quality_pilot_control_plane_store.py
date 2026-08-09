from __future__ import annotations

import ast
import inspect
import json
import unittest
from dataclasses import replace
from datetime import timedelta
from hashlib import sha256

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.quality_pilot import control_plane_store as store_module
from india_swing.quality_pilot.campaign_ledger import QualityPilotCampaignCompletenessLedger
from india_swing.quality_pilot.control_plane_store import (
    MAXIMUM_LEDGER_SNAPSHOT_BYTES,
    MAXIMUM_PLAN_BYTES,
    MAXIMUM_TRANSITION_BYTES,
    LoadedQualityPilotLedgerTransition,
    PinnedQualityPilotControlArtifactRequest,
    PinnedQualityPilotLedgerTransitionRequest,
    PublishedQualityPilotControlArtifact,
    QualityPilotCompletenessSnapshot,
    QualityPilotControlArtifactKind,
    QualityPilotControlStoreError,
    QualityPilotLedgerTransition,
    build_quality_pilot_completeness_snapshot,
    canonical_quality_pilot_transition_object_name,
    decode_quality_pilot_campaign_plan,
    decode_quality_pilot_completeness_snapshot,
    decode_quality_pilot_ledger_transition,
    encode_quality_pilot_campaign_plan,
    encode_quality_pilot_completeness_snapshot,
    encode_quality_pilot_ledger_transition,
    pinned_quality_pilot_ledger_transition_request,
    publish_quality_pilot_control_artifact,
    publish_quality_pilot_ledger_transition,
    read_pinned_quality_pilot_control_artifact,
    read_pinned_quality_pilot_ledger_transition,
)
from tests.test_quality_pilot_campaign_ledger import BUCKET, _at, _plan, _run
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter


class FakeReader:
    def __init__(self, content_bytes: bytes, generation: int) -> None:
        self.content_bytes = content_bytes
        self.generation = generation
        self.calls: list[dict[str, object]] = []

    def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
        self.calls.append(
            dict(
                bucket=bucket,
                object_name=object_name,
                generation=generation,
                maximum_bytes=maximum_bytes,
            )
        )
        return GCSObjectPayload(self.content_bytes, self.generation)


def _ledger():
    plan = _plan()
    runs = tuple(_run(spec) for spec in plan.capture_specs[:5])
    return QualityPilotCampaignCompletenessLedger(
        plan,
        runs,
        _at(plan.campaign.confirmed_sessions[0], 18, 0),
        BUCKET,
    )


class PlanCodecTests(unittest.TestCase):
    def test_round_trip_is_byte_exact_and_replayable(self) -> None:
        plan = _plan()
        encoded = encode_quality_pilot_campaign_plan(plan)
        decoded = decode_quality_pilot_campaign_plan(encoded)
        self.assertEqual(decoded.plan_id, plan.plan_id)
        self.assertEqual(decoded.capture_specs, plan.capture_specs)
        self.assertEqual(encode_quality_pilot_campaign_plan(decoded), encoded)

    def test_rejects_duplicate_float_tamper_and_noncanonical_bytes(self) -> None:
        encoded = encode_quality_pilot_campaign_plan(_plan())
        duplicate = encoded.replace(b'{"campaign":', b'{"campaign":{},"campaign":', 1)
        float_value = encoded.replace(b'"chunk_count":1', b'"chunk_count":1.0', 1)
        tree = json.loads(encoded)
        tree["plan_id"] = "0" * 64
        tampered = json.dumps(tree, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        noncanonical = json.dumps(json.loads(encoded), indent=2).encode()
        for value in (duplicate, float_value, tampered, noncanonical):
            with self.subTest(size=len(value)), self.assertRaises(QualityPilotControlStoreError):
                decode_quality_pilot_campaign_plan(value)

    def test_rejects_unbounded_or_wrong_content_type(self) -> None:
        for value in ("not-bytes", b"", b"x" * (MAXIMUM_PLAN_BYTES + 1)):
            with self.subTest(type=type(value)), self.assertRaises(QualityPilotControlStoreError):
                decode_quality_pilot_campaign_plan(value)  # type: ignore[arg-type]

    def test_decode_errors_are_static_and_have_no_exception_chain(self) -> None:
        for value in (b"\xff", b'{"schema_version":'):
            with self.assertRaises(QualityPilotControlStoreError) as raised:
                decode_quality_pilot_campaign_plan(value)
            self.assertIsNone(raised.exception.__cause__)
            self.assertIsNone(raised.exception.__context__)


class LedgerSnapshotTests(unittest.TestCase):
    def test_compact_snapshot_round_trip_preserves_all_restart_pins(self) -> None:
        ledger = _ledger()
        snapshot = build_quality_pilot_completeness_snapshot(ledger)
        encoded = encode_quality_pilot_completeness_snapshot(snapshot)
        decoded = decode_quality_pilot_completeness_snapshot(encoded)
        self.assertEqual(decoded.snapshot_id, snapshot.snapshot_id)
        self.assertEqual(decoded.ledger_id, ledger.ledger_id)
        self.assertEqual(decoded.run_result_ids, tuple(run.run_result_id for run in ledger.run_results))
        self.assertEqual(
            tuple(pin.expected_observation_id for pin in decoded.pinned_observations),
            tuple(run.observation.observation_id for run in ledger.run_results),
        )
        self.assertEqual(
            decoded.published_observation_byte_counts,
            tuple(run.published.encoded_byte_count for run in ledger.run_results),
        )
        self.assertNotIn(b'"payload"', encoded)
        self.assertLess(len(encoded), 20_000)

    def test_snapshot_rejects_tampered_identity_and_overlapping_state(self) -> None:
        snapshot = build_quality_pilot_completeness_snapshot(_ledger())
        with self.assertRaises(QualityPilotControlStoreError):
            replace(snapshot, pending_capture_spec_ids=(snapshot.completed_capture_spec_ids[0],))
        object.__setattr__(snapshot, "snapshot_id", "0" * 64)
        with self.assertRaises(QualityPilotControlStoreError):
            snapshot.verify_content_identity()

    def test_snapshot_decoder_rejects_duplicate_and_oversized_content(self) -> None:
        encoded = encode_quality_pilot_completeness_snapshot(
            build_quality_pilot_completeness_snapshot(_ledger())
        )
        duplicate = encoded.replace(b'{"bucket":', b'{"bucket":"x","bucket":', 1)
        for value in (duplicate, b"x" * (MAXIMUM_LEDGER_SNAPSHOT_BYTES + 1)):
            with self.assertRaises(QualityPilotControlStoreError):
                decode_quality_pilot_completeness_snapshot(value)


class ImmutableControlStoreTests(unittest.TestCase):
    def test_plan_publish_and_generation_pinned_reload(self) -> None:
        plan = _plan()
        writer = FakeStateObjectWriter()
        published = publish_quality_pilot_control_artifact(plan, BUCKET, writer)
        self.assertEqual(len(writer.calls), 1)
        self.assertEqual(writer.calls[0]["maximum_bytes"], MAXIMUM_PLAN_BYTES)
        self.assertEqual(published.kind, QualityPilotControlArtifactKind.CAMPAIGN_PLAN)
        request = PinnedQualityPilotControlArtifactRequest(
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
        reader = FakeReader(writer.calls[0]["content_bytes"], published.generation)
        loaded = read_pinned_quality_pilot_control_artifact(request, reader)
        self.assertEqual(loaded.artifact.plan_id, plan.plan_id)
        self.assertEqual(reader.calls[0]["maximum_bytes"], MAXIMUM_PLAN_BYTES)

    def test_ledger_publish_uses_distinct_immutable_lane(self) -> None:
        snapshot = build_quality_pilot_completeness_snapshot(_ledger())
        writer = FakeStateObjectWriter()
        published = publish_quality_pilot_control_artifact(snapshot, BUCKET, writer)
        self.assertIn("/control/ledgers/", published.object_name)
        self.assertEqual(writer.calls[0]["maximum_bytes"], MAXIMUM_LEDGER_SNAPSHOT_BYTES)

    def test_untrusted_writer_and_reader_fail_closed(self) -> None:
        plan = _plan()
        writer = FakeStateObjectWriter()
        writer.malicious_result = PublishedStateObject(
            object_name="wrong.json",
            generation=1,
            byte_count=1,
            sha256="0" * 64,
        )
        with self.assertRaises(QualityPilotControlStoreError):
            publish_quality_pilot_control_artifact(plan, BUCKET, writer)

        good_writer = FakeStateObjectWriter()
        published = publish_quality_pilot_control_artifact(plan, BUCKET, good_writer)
        request = PinnedQualityPilotControlArtifactRequest(
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
        content = good_writer.calls[0]["content_bytes"]
        for reader in (FakeReader(content, published.generation + 1), FakeReader(content + b" ", published.generation)):
            with self.assertRaises(QualityPilotControlStoreError):
                read_pinned_quality_pilot_control_artifact(request, reader)

    def test_module_cannot_list_select_latest_or_access_ambient_systems(self) -> None:
        source = inspect.getsource(store_module)
        tree = ast.parse(source)
        forbidden = {"os", "pathlib", "socket", "subprocess", "requests", "urllib", "google", "kiteconnect"}
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden, set())
        lowered = source.lower()
        for token in ("list_blobs", "latest", "getenv", "environ", "datetime.now", "place_order"):
            self.assertNotIn(token, lowered)


def _transition(*, previous_snapshot_id=None, run_results_count: int = 1) -> tuple[QualityPilotLedgerTransition, dict]:
    """Build one genuine, publishable transition for the first `run_results_count` specs."""

    plan = _plan()
    runs = tuple(_run(spec) for spec in plan.capture_specs[:run_results_count])
    ledger = QualityPilotCampaignCompletenessLedger(
        plan, runs, runs[-1].observation.request.request_ended_at, BUCKET
    )
    snapshot = build_quality_pilot_completeness_snapshot(ledger)
    writer = FakeStateObjectWriter()
    published_snapshot = publish_quality_pilot_control_artifact(snapshot, BUCKET, writer)
    transition = QualityPilotLedgerTransition(
        protocol_sha256=plan.campaign.protocol_sha256,
        pilot_run_id=plan.campaign.pilot_run_id,
        plan_id=plan.plan_id,
        previous_snapshot_id=previous_snapshot_id,
        capture_spec_id=plan.capture_specs[run_results_count - 1].capture_spec_id,
        run_result_id=runs[-1].run_result_id,
        next_snapshot=published_snapshot,
    )
    context = dict(plan=plan, runs=runs, ledger=ledger, snapshot=snapshot, writer=writer, published_snapshot=published_snapshot)
    return transition, context


class TransitionCodecTests(unittest.TestCase):
    def test_genesis_and_non_genesis_round_trip_is_byte_exact(self) -> None:
        genesis, _ = _transition(previous_snapshot_id=None)
        encoded = encode_quality_pilot_ledger_transition(genesis)
        decoded = decode_quality_pilot_ledger_transition(encoded)
        self.assertEqual(decoded.transition_id, genesis.transition_id)
        self.assertIsNone(decoded.previous_snapshot_id)
        self.assertEqual(encode_quality_pilot_ledger_transition(decoded), encoded)
        self.assertLess(len(encoded), MAXIMUM_TRANSITION_BYTES)

        non_genesis, _ = _transition(previous_snapshot_id="1" * 64)
        encoded2 = encode_quality_pilot_ledger_transition(non_genesis)
        decoded2 = decode_quality_pilot_ledger_transition(encoded2)
        self.assertEqual(decoded2.previous_snapshot_id, "1" * 64)
        self.assertEqual(encode_quality_pilot_ledger_transition(decoded2), encoded2)

    def test_decode_rejects_duplicate_keys_floats_and_noncanonical_bytes(self) -> None:
        encoded = encode_quality_pilot_ledger_transition(_transition()[0])
        duplicate = encoded.replace(b'{"capture_spec_id":', b'{"capture_spec_id":"x","capture_spec_id":', 1)
        float_value = encoded.replace(b'"generation":1', b'"generation":1.0', 1)
        tree = json.loads(encoded)
        tree["transition_id"] = "0" * 64
        tampered = json.dumps(tree, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        noncanonical = json.dumps(json.loads(encoded), indent=2).encode()
        for value in (duplicate, float_value, tampered, noncanonical):
            with self.subTest(size=len(value)), self.assertRaises(QualityPilotControlStoreError):
                decode_quality_pilot_ledger_transition(value)

    def test_decode_rejects_wrong_schema_malformed_and_uppercase_ids(self) -> None:
        encoded = encode_quality_pilot_ledger_transition(_transition()[0])
        wrong_schema = encoded.replace(b'"quality_pilot_ledger_transition_v1"', b'"quality_pilot_ledger_transition_v2"', 1)
        malformed = encoded.replace(b'"capture_spec_id":"', b'"capture_spec_id":"not-hex-', 1)
        for value in (wrong_schema, malformed):
            with self.subTest(value=value[:40]), self.assertRaises(QualityPilotControlStoreError):
                decode_quality_pilot_ledger_transition(value)

    def _malformed_next_snapshot(self, published, **field_overrides):
        """Bypass PublishedQualityPilotControlArtifact.__post_init__ to build a
        self-inconsistent nested artifact that would otherwise refuse to
        construct -- isolating QualityPilotLedgerTransition's own independent
        reconstruction defense from the nested type's own guard."""

        malformed = object.__new__(store_module.PublishedQualityPilotControlArtifact)
        for name in (
            "storage_policy_version", "protocol_sha256", "kind", "pilot_run_id",
            "artifact_id", "bucket", "object_name", "generation",
            "encoded_byte_count", "encoded_sha256",
        ):
            object.__setattr__(malformed, name, field_overrides.get(name, getattr(published, name)))
        return malformed

    def test_decode_rejects_foreign_artifact_kind_and_wrong_path(self) -> None:
        transition, context = _transition()
        published = context["published_snapshot"]
        foreign_kind = self._malformed_next_snapshot(
            published, kind=QualityPilotControlArtifactKind.CAMPAIGN_PLAN
        )
        with self.assertRaises(QualityPilotControlStoreError):
            QualityPilotLedgerTransition(
                protocol_sha256=transition.protocol_sha256,
                pilot_run_id=transition.pilot_run_id,
                plan_id=transition.plan_id,
                previous_snapshot_id=transition.previous_snapshot_id,
                capture_spec_id=transition.capture_spec_id,
                run_result_id=transition.run_result_id,
                next_snapshot=foreign_kind,
            )
        wrong_path = self._malformed_next_snapshot(published, object_name="quality-pilot/v1/wrong/path.json")
        with self.assertRaises(QualityPilotControlStoreError):
            QualityPilotLedgerTransition(
                protocol_sha256=transition.protocol_sha256,
                pilot_run_id=transition.pilot_run_id,
                plan_id=transition.plan_id,
                previous_snapshot_id=transition.previous_snapshot_id,
                capture_spec_id=transition.capture_spec_id,
                run_result_id=transition.run_result_id,
                next_snapshot=wrong_path,
            )

    def test_decode_rejects_invalid_generation_count_hash_and_oversized_bytes(self) -> None:
        encoded = encode_quality_pilot_ledger_transition(_transition()[0])
        bad_generation = encoded.replace(b'"generation":1', b'"generation":0', 1)
        bad_hash = encoded.replace(
            encoded[encoded.index(b'"encoded_sha256":"') + len(b'"encoded_sha256":"'): encoded.index(b'"encoded_sha256":"') + len(b'"encoded_sha256":"') + 64],
            b"0" * 64,
        )
        for value in (bad_generation, bad_hash, b"x" * (MAXIMUM_TRANSITION_BYTES + 1)):
            with self.subTest(size=len(value)), self.assertRaises(QualityPilotControlStoreError):
                decode_quality_pilot_ledger_transition(value)

    def test_posture_and_transition_id_tampering_fail_closed(self) -> None:
        transition, _ = _transition()
        object.__setattr__(transition, "transition_id", "0" * 64)
        with self.assertRaises(QualityPilotControlStoreError):
            transition.verify_content_identity()

        transition2, _ = _transition()
        with self.assertRaises(AttributeError):
            object.__setattr__(transition2, "quality_only", False)

    def test_nested_metadata_tampering_fails_closed(self) -> None:
        transition, context = _transition()
        published = context["published_snapshot"]
        tampered_protocol = self._malformed_next_snapshot(published, protocol_sha256="0" * 64)
        with self.assertRaises(QualityPilotControlStoreError):
            QualityPilotLedgerTransition(
                protocol_sha256=transition.protocol_sha256,
                pilot_run_id=transition.pilot_run_id,
                plan_id=transition.plan_id,
                previous_snapshot_id=transition.previous_snapshot_id,
                capture_spec_id=transition.capture_spec_id,
                run_result_id=transition.run_result_id,
                next_snapshot=tampered_protocol,
            )
        tampered_pilot_run = self._malformed_next_snapshot(published, pilot_run_id="1" * 64)
        with self.assertRaises(QualityPilotControlStoreError):
            QualityPilotLedgerTransition(
                protocol_sha256=transition.protocol_sha256,
                pilot_run_id=transition.pilot_run_id,
                plan_id=transition.plan_id,
                previous_snapshot_id=transition.previous_snapshot_id,
                capture_spec_id=transition.capture_spec_id,
                run_result_id=transition.run_result_id,
                next_snapshot=tampered_pilot_run,
            )


class TransitionPublicationTests(unittest.TestCase):
    def test_publishes_at_the_one_canonical_predecessor_spec_path(self) -> None:
        transition, context = _transition(previous_snapshot_id=None)
        writer = context["writer"]
        calls_before = len(writer.calls)
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        self.assertEqual(len(writer.calls) - calls_before, 1)
        call = writer.calls[-1]
        self.assertEqual(call["content_type"], "application/json")
        self.assertEqual(call["maximum_bytes"], MAXIMUM_TRANSITION_BYTES)
        expected_path = canonical_quality_pilot_transition_object_name(
            transition.pilot_run_id, transition.plan_id, "genesis", transition.capture_spec_id
        )
        self.assertEqual(published.object_name, expected_path)
        self.assertEqual(published.transition_id, transition.transition_id)

    def test_rejects_malicious_writer_type_path_generation_count_and_hash(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        writer.malicious_result = PublishedStateObject(
            object_name="wrong/path.json", generation=1, byte_count=1, sha256="0" * 64
        )
        with self.assertRaises(QualityPilotControlStoreError):
            publish_quality_pilot_ledger_transition(transition, BUCKET, writer)

    def test_writer_exception_with_planted_secret_is_sanitized(self) -> None:
        secret = "SECRET-TRANSITION-WRITER/C:/private/key"
        transition, context = _transition()
        writer = context["writer"]
        writer.raise_error = RuntimeError(secret)
        with self.assertRaises(QualityPilotControlStoreError) as raised:
            publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_byte_identical_reseal_is_idempotent(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        first = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        second = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        self.assertEqual(first.generation, second.generation)
        self.assertEqual(first.object_name, second.object_name)
        self.assertEqual(first.encoded_sha256, second.encoded_sha256)

    def test_different_successor_at_the_same_path_fails_closed(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        publish_quality_pilot_ledger_transition(transition, BUCKET, writer)

        different = QualityPilotLedgerTransition(
            protocol_sha256=transition.protocol_sha256,
            pilot_run_id=transition.pilot_run_id,
            plan_id=transition.plan_id,
            previous_snapshot_id=transition.previous_snapshot_id,
            capture_spec_id=transition.capture_spec_id,
            run_result_id="9" * 64,
            next_snapshot=context["published_snapshot"],
        )
        with self.assertRaises(QualityPilotControlStoreError):
            publish_quality_pilot_ledger_transition(different, BUCKET, writer)


class TransitionSelfCycleTests(unittest.TestCase):
    def test_rejects_next_snapshot_equal_to_its_own_predecessor(self) -> None:
        transition, context = _transition(previous_snapshot_id=None)
        published_snapshot = context["published_snapshot"]
        with self.assertRaises(QualityPilotControlStoreError):
            QualityPilotLedgerTransition(
                protocol_sha256=transition.protocol_sha256,
                pilot_run_id=transition.pilot_run_id,
                plan_id=transition.plan_id,
                # A transition claiming its own next_snapshot as its own predecessor.
                previous_snapshot_id=published_snapshot.artifact_id,
                capture_spec_id=transition.capture_spec_id,
                run_result_id=transition.run_result_id,
                next_snapshot=published_snapshot,
            )

    def test_genesis_transition_is_unaffected_by_the_self_cycle_check(self) -> None:
        transition, _ = _transition(previous_snapshot_id=None)
        self.assertIsNone(transition.previous_snapshot_id)
        transition.verify_content_identity()


class FixedPostureStructuralImmutabilityTests(unittest.TestCase):
    """Every _FixedPostureMixin subtype in this module must be structurally
    immune to object.__setattr__ tampering on every posture name, not only
    quality_only -- and direct reads must always remain fixed."""

    def _assert_all_posture_names_are_immutable(self, value) -> None:
        for name in (
            "quality_only",
            "counts_toward_o0",
            "counts_toward_clean_accumulation",
            "research_partition_eligible",
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "signal_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
            "capital_eligible",
        ):
            with self.subTest(name=name):
                before = getattr(value, name)
                with self.assertRaises(AttributeError):
                    object.__setattr__(value, name, not before)
                self.assertEqual(getattr(value, name), before)

    def test_completeness_snapshot_posture_is_structurally_immutable(self) -> None:
        snapshot = build_quality_pilot_completeness_snapshot(_ledger())
        self._assert_all_posture_names_are_immutable(snapshot)

    def test_published_control_artifact_posture_is_structurally_immutable(self) -> None:
        published = publish_quality_pilot_control_artifact(_plan(), BUCKET, FakeStateObjectWriter())
        self._assert_all_posture_names_are_immutable(published)

    def test_ledger_transition_posture_is_structurally_immutable(self) -> None:
        transition, _ = _transition()
        self._assert_all_posture_names_are_immutable(transition)

    def test_published_ledger_transition_posture_is_structurally_immutable(self) -> None:
        transition, context = _transition()
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, context["writer"])
        self._assert_all_posture_names_are_immutable(published)

    def test_identity_verification_still_passes_for_untampered_values(self) -> None:
        snapshot = build_quality_pilot_completeness_snapshot(_ledger())
        snapshot.verify_content_identity()
        transition, _ = _transition()
        transition.verify_content_identity()


class TransitionReadTests(unittest.TestCase):
    def test_pinned_read_happy_path_proves_exact_route_and_replay(self) -> None:
        transition, context = _transition(previous_snapshot_id=None)
        writer = context["writer"]
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        content = writer.store[(published.bucket, published.object_name)][0]
        reader = FakeReader(content, published.generation)
        request = pinned_quality_pilot_ledger_transition_request(published)
        expected_path = canonical_quality_pilot_transition_object_name(
            transition.pilot_run_id, transition.plan_id, "genesis", transition.capture_spec_id
        )
        self.assertEqual(request.object_name, expected_path)
        loaded = read_pinned_quality_pilot_ledger_transition(request, reader)
        self.assertIsInstance(loaded, LoadedQualityPilotLedgerTransition)
        self.assertEqual(loaded.transition.transition_id, transition.transition_id)
        self.assertEqual(reader.calls[0]["maximum_bytes"], MAXIMUM_TRANSITION_BYTES)
        self.assertEqual(len(reader.calls), 1)

    def test_rejects_wrong_generation_reported_by_the_reader(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        content = writer.store[(published.bucket, published.object_name)][0]
        request = pinned_quality_pilot_ledger_transition_request(published)
        # The pin asks for the true generation, but the reader lies and
        # returns a different one.
        reader = FakeReader(content, published.generation + 1)
        with self.assertRaises(QualityPilotControlStoreError):
            read_pinned_quality_pilot_ledger_transition(request, reader)

    def test_rejects_bool_generation_reported_by_the_reader(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        content = writer.store[(published.bucket, published.object_name)][0]
        request = pinned_quality_pilot_ledger_transition_request(published)

        class BoolGenerationReader:
            def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
                return GCSObjectPayload(content, True)

        with self.assertRaises(QualityPilotControlStoreError):
            read_pinned_quality_pilot_ledger_transition(request, BoolGenerationReader())

    def test_rejects_hostile_generation_type(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        content = writer.store[(published.bucket, published.object_name)][0]
        request = pinned_quality_pilot_ledger_transition_request(published)

        class HostileGeneration:
            def __eq__(self, other):
                raise RuntimeError("SECRET-GENERATION-COMPARATOR")

            def __ne__(self, other):
                raise RuntimeError("SECRET-GENERATION-COMPARATOR")

        class HostileReader:
            def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
                return GCSObjectPayload(content, HostileGeneration())

        with self.assertRaises(QualityPilotControlStoreError) as raised:
            read_pinned_quality_pilot_ledger_transition(request, HostileReader())
        self.assertNotIn("SECRET-GENERATION-COMPARATOR", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_rejects_wrong_hash_path_transition_plan_predecessor_capture_bucket(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        content = writer.store[(published.bucket, published.object_name)][0]
        base_request = pinned_quality_pilot_ledger_transition_request(published)

        wrong_hash_reader = FakeReader(content + b" ", published.generation)
        with self.assertRaises(QualityPilotControlStoreError):
            read_pinned_quality_pilot_ledger_transition(base_request, wrong_hash_reader)

        for field_name, bad_value in (
            ("transition_id", "0" * 64),
            ("plan_id", "1" * 64),
            ("capture_spec_id", "2" * 64),
        ):
            with self.subTest(field=field_name):
                malformed = object.__new__(PinnedQualityPilotLedgerTransitionRequest)
                for name in (
                    "storage_policy_version", "protocol_sha256", "pilot_run_id", "plan_id",
                    "previous_snapshot_id", "capture_spec_id", "transition_id", "bucket",
                    "object_name", "generation", "expected_encoded_sha256",
                ):
                    object.__setattr__(malformed, name, getattr(base_request, name))
                object.__setattr__(malformed, field_name, bad_value)
                reader = FakeReader(content, published.generation)
                with self.assertRaises(QualityPilotControlStoreError):
                    read_pinned_quality_pilot_ledger_transition(malformed, reader)

    def test_rejects_non_bytes_empty_oversized_and_malformed_content(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        request = pinned_quality_pilot_ledger_transition_request(published)
        for content in ("not-bytes", b"", b"x" * (MAXIMUM_TRANSITION_BYTES + 1), b"not-json"):
            with self.subTest(kind=type(content)):
                reader = FakeReader(content, published.generation) if type(content) is bytes else FakeReader(b"placeholder", published.generation)
                if type(content) is not bytes:
                    reader.content_bytes = content
                else:
                    reader.content_bytes = content
                with self.assertRaises(QualityPilotControlStoreError):
                    read_pinned_quality_pilot_ledger_transition(request, reader)

    def test_rejects_foreign_payload_and_transition_types(self) -> None:
        transition, context = _transition()
        writer = context["writer"]
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        request = pinned_quality_pilot_ledger_transition_request(published)

        class ForeignPayload:
            content_bytes = b"{}"
            generation = 1

        class ForeignPayloadReader:
            def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
                return ForeignPayload()

        with self.assertRaises(QualityPilotControlStoreError):
            read_pinned_quality_pilot_ledger_transition(request, ForeignPayloadReader())

        class WrongArtifactReader:
            def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
                # A byte-valid, hash-mismatched payload masquerading as the transition.
                content = writer.store[(published.bucket, published.object_name)][0]
                return GCSObjectPayload(content, generation)

        mismatched_request = replace(request, expected_encoded_sha256="0" * 64)
        with self.assertRaises(QualityPilotControlStoreError):
            read_pinned_quality_pilot_ledger_transition(mismatched_request, WrongArtifactReader())

    def test_planted_secret_reader_exception_is_sanitized(self) -> None:
        secret = "SECRET-TRANSITION-READER/C:/private/token"
        transition, context = _transition()
        writer = context["writer"]
        published = publish_quality_pilot_ledger_transition(transition, BUCKET, writer)
        request = pinned_quality_pilot_ledger_transition_request(published)

        class BoomReader:
            def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
                raise RuntimeError(secret)

        with self.assertRaises(QualityPilotControlStoreError) as raised:
            read_pinned_quality_pilot_ledger_transition(request, BoomReader())
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)


if __name__ == "__main__":
    unittest.main()
