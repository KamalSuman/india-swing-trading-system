from __future__ import annotations

import ast
import dataclasses
import hashlib
import inspect
import json
import tempfile
import unittest
from pathlib import Path

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    build_promoted_operational_advisory,
    build_promoted_operational_terminal_record,
    promoted_paper_registration_from_result,
)

import india_swing.promoted_operational_gcs_state as gs_module
from india_swing.promoted_operational_gcs_state import (
    CompletedPromotedOperationalGCSPublication,
    CompletedPromotedOperationalGCSRestore,
    PromotedOperationalGCSArtifact,
    PromotedOperationalGCSArtifactKind,
    PromotedOperationalGCSManifest,
    PromotedOperationalGCSRestoreRequest,
    PromotedOperationalGCSStateError,
    decode_promoted_operational_gcs_manifest,
    encode_promoted_operational_gcs_manifest,
    promoted_operational_gcs_artifact_object_name,
    promoted_operational_gcs_manifest_object_name,
    publish_promoted_operational_state_to_gcs,
    restore_promoted_operational_state_from_gcs,
)

from tests.test_promoted_operational_persistence import (
    _complete_no_trade_result,
    _complete_paper_buy_result,
    _failed_result,
)

BUCKET = "promoted-operational-test-bucket"


class FakeWriter:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], tuple[bytes, int]] = {}
        self.calls: list[str] = []
        self.raise_error: Exception | None = None
        self.malicious_result: object = None

    def create_or_verify(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
        self.calls.append(object_name)
        if self.raise_error is not None:
            raise self.raise_error
        if self.malicious_result is not None:
            return self.malicious_result
        key = (bucket, object_name)
        if key in self.store:
            existing_bytes, generation = self.store[key]
            if existing_bytes != content_bytes:
                raise ValueError("writer content mismatch at existing path")
            return PublishedStateObject(
                object_name=object_name, generation=generation,
                byte_count=len(existing_bytes), sha256=hashlib.sha256(existing_bytes).hexdigest(),
            )
        generation = len(self.store) + 1
        self.store[key] = (content_bytes, generation)
        return PublishedStateObject(
            object_name=object_name, generation=generation,
            byte_count=len(content_bytes), sha256=hashlib.sha256(content_bytes).hexdigest(),
        )


class FakeReader:
    def __init__(self, writer: FakeWriter) -> None:
        self.writer = writer
        self.calls: list[dict[str, object]] = []
        self.raise_error: Exception | None = None
        self.override: dict[str, object] | None = None

    def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
        self.calls.append(
            dict(bucket=bucket, object_name=object_name, generation=generation, maximum_bytes=maximum_bytes)
        )
        if self.raise_error is not None:
            raise self.raise_error
        if self.override is not None and self.override.get("object_name") == object_name:
            return self.override["payload"]
        content_bytes, stored_generation = self.writer.store[(bucket, object_name)]
        if stored_generation != generation:
            raise ValueError("simulated generation mismatch")
        return GCSObjectPayload(content_bytes=content_bytes, generation=generation)


def _build(result):
    advisory = build_promoted_operational_advisory(result)
    registration = promoted_paper_registration_from_result(result, advisory)
    terminal = build_promoted_operational_terminal_record(result, advisory, registration)
    return advisory, registration, terminal


class _Fixture:
    def __init__(self, result_factory) -> None:
        self.result = result_factory()
        self.advisory, self.registration, self.terminal = _build(self.result)
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(root)
        self.paper_ledger = LocalPaperTradeLedger(root)
        self.advisory_outbox.put(self.advisory)
        if self.registration is not None:
            self.paper_ledger.register_value(self.registration)
        self.writer = FakeWriter()

    def close(self) -> None:
        self.tmpdir.cleanup()

    def publish(self) -> CompletedPromotedOperationalGCSPublication:
        return publish_promoted_operational_state_to_gcs(
            terminal=self.terminal, bucket=BUCKET, writer=self.writer,
            advisory_outbox=self.advisory_outbox, paper_ledger=self.paper_ledger,
        )

    def restore_request(self, publication: CompletedPromotedOperationalGCSPublication):
        manifest_object = publication.manifest_object
        return PromotedOperationalGCSRestoreRequest(
            bucket=BUCKET, manifest_object_name=manifest_object.object_name,
            generation=manifest_object.generation, expected_sha256=manifest_object.sha256,
            expected_spec_id=self.terminal.spec_id,
        )

    def fresh_local_stores(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        return (
            tmp,
            LocalPromotedOperationalTerminalStore(root),
            LocalPromotedOperationalAdvisoryOutbox(root),
            LocalPaperTradeLedger(root),
        )


class ManifestCodecTests(unittest.TestCase):
    def test_paper_buy_manifest_round_trip_and_canonical_artifact_order(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            manifest = publication.manifest
            self.assertEqual(
                [a.kind for a in manifest.artifacts],
                [
                    PromotedOperationalGCSArtifactKind.ADVISORY,
                    PromotedOperationalGCSArtifactKind.PAPER_REGISTRATION,
                    PromotedOperationalGCSArtifactKind.TERMINAL,
                ],
            )
            encoded = encode_promoted_operational_gcs_manifest(manifest)
            reloaded = decode_promoted_operational_gcs_manifest(encoded)
            self.assertEqual(reloaded.publication_id, manifest.publication_id)
            self.assertEqual(encode_promoted_operational_gcs_manifest(reloaded), encoded)
        finally:
            fx.close()

    def test_no_trade_and_failed_manifests_omit_paper_registration_artifact(self) -> None:
        for factory in (_complete_no_trade_result, _failed_result):
            fx = _Fixture(factory)
            try:
                publication = fx.publish()
                self.assertEqual(
                    [a.kind for a in publication.manifest.artifacts],
                    [PromotedOperationalGCSArtifactKind.ADVISORY, PromotedOperationalGCSArtifactKind.TERMINAL],
                )
                self.assertIsNone(publication.manifest.paper_registration_id)
            finally:
                fx.close()

    def test_deterministic_publication_identity_is_stable_across_rebuilds(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication1 = fx.publish()
            fx2 = _Fixture(_complete_paper_buy_result)
            try:
                publication2 = fx2.publish()
                self.assertEqual(publication1.manifest.publication_id, publication2.manifest.publication_id)
            finally:
                fx2.close()
        finally:
            fx.close()

    def test_decode_rejects_duplicate_keys(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            encoded = encode_promoted_operational_gcs_manifest(manifest)
            text = encoded.decode("utf-8")
            tampered = text[:-2] + f',"spec_id":"{manifest.spec_id}"}}\n'
            with self.assertRaises(PromotedOperationalGCSStateError):
                decode_promoted_operational_gcs_manifest(tampered.encode("utf-8"))
        finally:
            fx.close()

    def test_decode_rejects_unknown_keys(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            encoded = encode_promoted_operational_gcs_manifest(manifest)
            text = encoded.decode("utf-8")[:-2] + ',"extra_field":"x"}\n'
            with self.assertRaises(PromotedOperationalGCSStateError):
                decode_promoted_operational_gcs_manifest(text.encode("utf-8"))
        finally:
            fx.close()

    def test_decode_rejects_missing_keys(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            root = json.loads(encode_promoted_operational_gcs_manifest(manifest).decode("utf-8"))
            del root["preparation_id"]
            tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            with self.assertRaises(PromotedOperationalGCSStateError):
                decode_promoted_operational_gcs_manifest(tampered)
        finally:
            fx.close()

    def test_decode_rejects_float_literal(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            text = encode_promoted_operational_gcs_manifest(manifest).decode("utf-8")
            tampered = text.replace('"schema_version":1', '"schema_version":1.0')
            with self.assertRaises(PromotedOperationalGCSStateError):
                decode_promoted_operational_gcs_manifest(tampered.encode("utf-8"))
        finally:
            fx.close()

    def test_decode_rejects_nan_literal(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            text = encode_promoted_operational_gcs_manifest(manifest).decode("utf-8")
            tampered = text.replace('"schema_version":1', '"schema_version":NaN')
            with self.assertRaises(PromotedOperationalGCSStateError):
                decode_promoted_operational_gcs_manifest(tampered.encode("utf-8"))
        finally:
            fx.close()

    def test_decode_rejects_invalid_utf8(self) -> None:
        with self.assertRaises(PromotedOperationalGCSStateError):
            decode_promoted_operational_gcs_manifest(b"\xff\xfe not utf-8")

    def test_decode_rejects_noncanonical_json(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            pretty = encode_promoted_operational_gcs_manifest(manifest).decode("utf-8").replace(",", ", ")
            with self.assertRaises(PromotedOperationalGCSStateError):
                decode_promoted_operational_gcs_manifest(pretty.encode("utf-8"))
        finally:
            fx.close()

    def test_decode_rejects_empty_and_oversized_bytes(self) -> None:
        with self.assertRaises(PromotedOperationalGCSStateError):
            decode_promoted_operational_gcs_manifest(b"")
        with self.assertRaises(PromotedOperationalGCSStateError):
            decode_promoted_operational_gcs_manifest(b"{}" + b" " * (300 * 1024))

    def test_manifest_rejects_subclassed_artifact(self) -> None:
        class _SubclassedArtifact(PromotedOperationalGCSArtifact):
            pass

        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            real_artifact = manifest.artifacts[0]
            subclassed = _SubclassedArtifact(kind=real_artifact.kind, published_object=real_artifact.published_object)
            with self.assertRaises(PromotedOperationalGCSStateError):
                PromotedOperationalGCSManifest(
                    bucket=manifest.bucket, spec_id=manifest.spec_id, preparation_id=manifest.preparation_id,
                    target_session=manifest.target_session, terminal_id=manifest.terminal_id,
                    status=manifest.status, action=manifest.action, advisory_id=manifest.advisory_id,
                    paper_registration_id=manifest.paper_registration_id,
                    artifacts=(subclassed,) + manifest.artifacts[1:],
                )
        finally:
            fx.close()

    def test_manifest_rejects_bool_generation(self) -> None:
        # Bool-generation rejection is a property of the already-accepted
        # PublishedStateObject type this module reuses unchanged (never
        # widened or bypassed) -- this proves the protection is still
        # reachable through this module's own artifact/manifest path.
        from india_swing.daily_pipeline.state_publication import StatePublicationError

        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            real_artifact = manifest.artifacts[0]
            with self.assertRaises(StatePublicationError):
                PublishedStateObject(
                    object_name=real_artifact.published_object.object_name, generation=True,
                    byte_count=real_artifact.published_object.byte_count, sha256=real_artifact.published_object.sha256,
                )
        finally:
            fx.close()

    def test_manifest_rejects_wrong_artifact_order(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            reordered = tuple(reversed(manifest.artifacts))
            with self.assertRaises(PromotedOperationalGCSStateError):
                PromotedOperationalGCSManifest(
                    bucket=manifest.bucket, spec_id=manifest.spec_id, preparation_id=manifest.preparation_id,
                    target_session=manifest.target_session, terminal_id=manifest.terminal_id,
                    status=manifest.status, action=manifest.action, advisory_id=manifest.advisory_id,
                    paper_registration_id=manifest.paper_registration_id, artifacts=reordered,
                )
        finally:
            fx.close()

    def test_manifest_rejects_duplicate_artifact(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            manifest = fx.publish().manifest
            duplicated = manifest.artifacts + (manifest.artifacts[-1],)
            with self.assertRaises(PromotedOperationalGCSStateError):
                PromotedOperationalGCSManifest(
                    bucket=manifest.bucket, spec_id=manifest.spec_id, preparation_id=manifest.preparation_id,
                    target_session=manifest.target_session, terminal_id=manifest.terminal_id,
                    status=manifest.status, action=manifest.action, advisory_id=manifest.advisory_id,
                    paper_registration_id=manifest.paper_registration_id, artifacts=duplicated,
                )
        finally:
            fx.close()

    def test_manifest_rejects_missing_artifact(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            manifest = fx.publish().manifest
            with self.assertRaises(PromotedOperationalGCSStateError):
                PromotedOperationalGCSManifest(
                    bucket=manifest.bucket, spec_id=manifest.spec_id, preparation_id=manifest.preparation_id,
                    target_session=manifest.target_session, terminal_id=manifest.terminal_id,
                    status=manifest.status, action=manifest.action, advisory_id=manifest.advisory_id,
                    paper_registration_id=manifest.paper_registration_id, artifacts=manifest.artifacts[:1],
                )
        finally:
            fx.close()

    def test_manifest_rejects_extra_artifact_without_matching_paper_registration_id(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            with self.assertRaises(PromotedOperationalGCSStateError):
                PromotedOperationalGCSManifest(
                    bucket=manifest.bucket, spec_id=manifest.spec_id, preparation_id=manifest.preparation_id,
                    target_session=manifest.target_session, terminal_id=manifest.terminal_id,
                    status=manifest.status, action=manifest.action, advisory_id=manifest.advisory_id,
                    paper_registration_id=None, artifacts=manifest.artifacts,
                )
        finally:
            fx.close()

    def test_manifest_rejects_mismatched_authority_status_action(self) -> None:
        fx = _Fixture(_failed_result)
        try:
            manifest = fx.publish().manifest
            from india_swing.promoted_operational_decision import PromotedOperationalDecisionAction

            with self.assertRaises(PromotedOperationalGCSStateError):
                PromotedOperationalGCSManifest(
                    bucket=manifest.bucket, spec_id=manifest.spec_id, preparation_id=manifest.preparation_id,
                    target_session=manifest.target_session, terminal_id=manifest.terminal_id,
                    status=manifest.status, action=PromotedOperationalDecisionAction.PAPER_BUY,
                    advisory_id=manifest.advisory_id, paper_registration_id=manifest.paper_registration_id,
                    artifacts=manifest.artifacts,
                )
        finally:
            fx.close()

    def test_artifact_object_name_uses_versioned_layout(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            manifest = fx.publish().manifest
            name = promoted_operational_gcs_artifact_object_name(manifest, PromotedOperationalGCSArtifactKind.ADVISORY)
            self.assertEqual(
                name,
                f"promoted-operational-state/v1/{manifest.target_session.isoformat()}/{manifest.spec_id}/"
                f"artifacts/advisory/{manifest.advisory_id}.json",
            )
            manifest_name = promoted_operational_gcs_manifest_object_name(manifest)
            self.assertEqual(
                manifest_name,
                f"promoted-operational-state/v1/{manifest.target_session.isoformat()}/{manifest.spec_id}/"
                f"manifests/{manifest.publication_id}.json",
            )
        finally:
            fx.close()


class PublicationTests(unittest.TestCase):
    def test_resolves_exact_ids_and_writes_in_canonical_order(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            self.assertEqual(len(fx.writer.calls), 4)
            self.assertIn("/artifacts/advisory/", fx.writer.calls[0])
            self.assertIn("/artifacts/paper_registration/", fx.writer.calls[1])
            self.assertIn("/artifacts/terminal/", fx.writer.calls[2])
            self.assertIn("/manifests/", fx.writer.calls[3])
            for artifact in publication.manifest.artifacts:
                self.assertIn(artifact.published_object.object_name, fx.writer.calls)
        finally:
            fx.close()

    def test_no_trade_writes_exactly_advisory_then_terminal_then_manifest(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            fx.publish()
            self.assertEqual(len(fx.writer.calls), 3)
            self.assertIn("/artifacts/advisory/", fx.writer.calls[0])
            self.assertIn("/artifacts/terminal/", fx.writer.calls[1])
            self.assertIn("/manifests/", fx.writer.calls[2])
        finally:
            fx.close()

    def test_publish_is_idempotent_under_create_or_verify_fake(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication1 = fx.publish()
            publication2 = fx.publish()
            self.assertEqual(publication1.manifest.publication_id, publication2.manifest.publication_id)
        finally:
            fx.close()

    def test_rejects_malicious_published_state_object_return(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            fx.writer.malicious_result = "not-a-published-state-object"
            with self.assertRaises(PromotedOperationalGCSStateError):
                fx.publish()
        finally:
            fx.close()

    def test_missing_advisory_fails_before_any_writer_call(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            fresh_root = Path(tempfile.mkdtemp())
            empty_outbox = LocalPromotedOperationalAdvisoryOutbox(fresh_root)
            with self.assertRaises(PromotedOperationalGCSStateError):
                publish_promoted_operational_state_to_gcs(
                    terminal=fx.terminal, bucket=BUCKET, writer=fx.writer,
                    advisory_outbox=empty_outbox, paper_ledger=fx.paper_ledger,
                )
            self.assertEqual(fx.writer.calls, [])
        finally:
            fx.close()

    def test_missing_registration_fails_before_any_writer_call(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            fresh_root = Path(tempfile.mkdtemp())
            empty_ledger = LocalPaperTradeLedger(fresh_root)
            with self.assertRaises(PromotedOperationalGCSStateError):
                publish_promoted_operational_state_to_gcs(
                    terminal=fx.terminal, bucket=BUCKET, writer=fx.writer,
                    advisory_outbox=fx.advisory_outbox, paper_ledger=empty_ledger,
                )
            self.assertEqual(fx.writer.calls, [])
        finally:
            fx.close()

    def test_writer_failure_at_advisory_stops_before_later_writes(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            fx.writer.raise_error = RuntimeError("boom")
            with self.assertRaises(PromotedOperationalGCSStateError):
                fx.publish()
            self.assertEqual(len(fx.writer.calls), 1)
        finally:
            fx.close()

    def test_writer_failure_at_manifest_stops_after_all_artifacts(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            original_create = fx.writer.create_or_verify

            def _fail_on_manifest(*, bucket, object_name, content_bytes, content_type, maximum_bytes):
                if "/manifests/" in object_name:
                    fx.writer.calls.append(object_name)
                    raise RuntimeError("boom")
                return original_create(
                    bucket=bucket, object_name=object_name, content_bytes=content_bytes,
                    content_type=content_type, maximum_bytes=maximum_bytes,
                )

            fx.writer.create_or_verify = _fail_on_manifest
            with self.assertRaises(PromotedOperationalGCSStateError):
                fx.publish()
            self.assertEqual(len(fx.writer.calls), 3)
        finally:
            fx.close()


class RestoreTests(unittest.TestCase):
    def test_restore_reaches_every_reader_call_with_exact_generation_and_bounds(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                restore_promoted_operational_state_from_gcs(
                    request=request, reader=reader, terminal_store=terminal_store,
                    advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                )
                self.assertEqual(len(reader.calls), 4)
                for call in reader.calls:
                    self.assertGreater(call["generation"], 0)
                    self.assertGreater(call["maximum_bytes"], 0)
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_externally_pinned_manifest_hash_is_authoritative(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = PromotedOperationalGCSRestoreRequest(
                bucket=BUCKET, manifest_object_name=publication.manifest_object.object_name,
                generation=publication.manifest_object.generation, expected_sha256="9" * 64,
                expected_spec_id=fx.terminal.spec_id,
            )
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                with self.assertRaises(PromotedOperationalGCSStateError):
                    restore_promoted_operational_state_from_gcs(
                        request=request, reader=reader, terminal_store=terminal_store,
                        advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                    )
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_completed_restore_reconstructs_all_nested_values(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                restore = restore_promoted_operational_state_from_gcs(
                    request=request, reader=reader, terminal_store=terminal_store,
                    advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                )
                self.assertIsInstance(restore, CompletedPromotedOperationalGCSRestore)
                self.assertEqual(restore.terminal.terminal_id, fx.terminal.terminal_id)
                self.assertEqual(restore.advisory.advisory_id, fx.advisory.advisory_id)
                self.assertEqual(restore.paper_registration.registration_id, fx.registration.registration_id)
                self.assertEqual(restore.manifest.publication_id, publication.manifest.publication_id)
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_advisory_and_registration_are_restored_before_terminal(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                order: list[str] = []
                original_advisory_put = advisory_outbox.put
                original_register = paper_ledger.register_value
                original_terminal_put = terminal_store.put

                def _tracked_advisory_put(value):
                    order.append("advisory")
                    return original_advisory_put(value)

                def _tracked_register(value):
                    order.append("registration")
                    return original_register(value)

                def _tracked_terminal_put(value):
                    order.append("terminal")
                    return original_terminal_put(value)

                advisory_outbox.put = _tracked_advisory_put
                paper_ledger.register_value = _tracked_register
                terminal_store.put = _tracked_terminal_put

                restore_promoted_operational_state_from_gcs(
                    request=request, reader=reader, terminal_store=terminal_store,
                    advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                )
                self.assertEqual(order, ["advisory", "registration", "terminal"])
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_wrong_manifest_generation_is_rejected(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = PromotedOperationalGCSRestoreRequest(
                bucket=BUCKET, manifest_object_name=publication.manifest_object.object_name,
                generation=publication.manifest_object.generation + 1,
                expected_sha256=publication.manifest_object.sha256, expected_spec_id=fx.terminal.spec_id,
            )
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                with self.assertRaises(PromotedOperationalGCSStateError):
                    restore_promoted_operational_state_from_gcs(
                        request=request, reader=reader, terminal_store=terminal_store,
                        advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                    )
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_wrong_expected_spec_id_is_rejected_at_request_construction(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            publication = fx.publish()
            with self.assertRaises(PromotedOperationalGCSStateError):
                PromotedOperationalGCSRestoreRequest(
                    bucket=BUCKET, manifest_object_name=publication.manifest_object.object_name,
                    generation=publication.manifest_object.generation,
                    expected_sha256=publication.manifest_object.sha256, expected_spec_id="9" * 64,
                )
        finally:
            fx.close()

    def test_wrong_bucket_in_request_is_rejected(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = PromotedOperationalGCSRestoreRequest(
                bucket="a-different-bucket", manifest_object_name=publication.manifest_object.object_name,
                generation=publication.manifest_object.generation,
                expected_sha256=publication.manifest_object.sha256, expected_spec_id=fx.terminal.spec_id,
            )
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                with self.assertRaises((PromotedOperationalGCSStateError, KeyError)):
                    restore_promoted_operational_state_from_gcs(
                        request=request, reader=reader, terminal_store=terminal_store,
                        advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                    )
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_truncated_manifest_object_name_is_rejected(self) -> None:
        with self.assertRaises(PromotedOperationalGCSStateError):
            PromotedOperationalGCSRestoreRequest(
                bucket=BUCKET, manifest_object_name="promoted-operational-state/v1/not-a-real-path.json",
                generation=1, expected_sha256="a" * 64, expected_spec_id="b" * 64,
            )

    def test_wrong_artifact_generation_is_rejected(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            reader.override = {
                "object_name": publication.manifest.artifacts[0].published_object.object_name,
                "payload": GCSObjectPayload(
                    content_bytes=fx.writer.store[
                        (BUCKET, publication.manifest.artifacts[0].published_object.object_name)
                    ][0],
                    generation=999,
                ),
            }
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                with self.assertRaises(PromotedOperationalGCSStateError):
                    restore_promoted_operational_state_from_gcs(
                        request=request, reader=reader, terminal_store=terminal_store,
                        advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                    )
                self.assertFalse(terminal_store.path_for(fx.terminal.spec_id).exists())
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_tampered_artifact_content_fails_hash_check(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            advisory_object_name = publication.manifest.artifacts[0].published_object.object_name
            key = (BUCKET, advisory_object_name)
            tampered_bytes = fx.writer.store[key][0].replace(b"advisory_text", b"advisory_TEXT")
            reader.override = {
                "object_name": advisory_object_name,
                "payload": GCSObjectPayload(content_bytes=tampered_bytes, generation=fx.writer.store[key][1]),
            }
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                with self.assertRaises(PromotedOperationalGCSStateError):
                    restore_promoted_operational_state_from_gcs(
                        request=request, reader=reader, terminal_store=terminal_store,
                        advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                    )
                self.assertFalse(terminal_store.path_for(fx.terminal.spec_id).exists())
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_reader_failure_leaves_terminal_store_untouched(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            reader.raise_error = RuntimeError("boom")
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                with self.assertRaises(PromotedOperationalGCSStateError):
                    restore_promoted_operational_state_from_gcs(
                        request=request, reader=reader, terminal_store=terminal_store,
                        advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                    )
                self.assertFalse(terminal_store.path_for(fx.terminal.spec_id).exists())
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_cross_linked_but_self_consistent_foreign_bundle_is_rejected(self) -> None:
        # Publish two independently valid results, then forge a manifest for
        # result A whose advisory_id/paper_registration_id point at result
        # B's genuinely self-consistent, independently valid artifacts.
        fx_a = _Fixture(_complete_paper_buy_result)
        fx_b = _Fixture(_complete_paper_buy_result)
        try:
            publication_a = fx_a.publish()
            fx_b.publish()

            forged_manifest_fields = dict(
                bucket=publication_a.manifest.bucket, spec_id=publication_a.manifest.spec_id,
                preparation_id=publication_a.manifest.preparation_id,
                target_session=publication_a.manifest.target_session,
                terminal_id=publication_a.manifest.terminal_id, status=publication_a.manifest.status,
                action=publication_a.manifest.action,
                advisory_id=fx_b.advisory.advisory_id,
                paper_registration_id=fx_b.registration.registration_id,
            )
            with self.assertRaises(PromotedOperationalGCSStateError):
                PromotedOperationalGCSManifest(
                    **forged_manifest_fields,
                    artifacts=(
                        PromotedOperationalGCSArtifact(
                            kind=PromotedOperationalGCSArtifactKind.ADVISORY,
                            published_object=next(
                                a.published_object for a in fx_b.publish().manifest.artifacts
                                if a.kind is PromotedOperationalGCSArtifactKind.ADVISORY
                            ),
                        ),
                    ),
                )
        finally:
            fx_a.close()
            fx_b.close()


class CrashReplayTests(unittest.TestCase):
    def test_artifacts_without_a_manifest_cannot_be_restored(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            # Publish only the advisory artifact by crashing the writer
            # before the terminal/manifest are ever written.
            original_create = fx.writer.create_or_verify

            def _stop_after_advisory(*, bucket, object_name, content_bytes, content_type, maximum_bytes):
                if "/artifacts/advisory/" in object_name:
                    return original_create(
                        bucket=bucket, object_name=object_name, content_bytes=content_bytes,
                        content_type=content_type, maximum_bytes=maximum_bytes,
                    )
                raise RuntimeError("simulated crash")

            fx.writer.create_or_verify = _stop_after_advisory
            with self.assertRaises(PromotedOperationalGCSStateError):
                fx.publish()

            # No manifest was ever sealed, so there is nothing to restore --
            # confirm the writer never received a manifest write.
            self.assertFalse(any("/manifests/" in call for call in fx.writer.calls))
        finally:
            fx.close()

    def test_failure_after_local_parent_restore_but_before_terminal_is_idempotent_on_replay(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                original_terminal_put = terminal_store.put
                calls = {"count": 0}

                def _crash_once(value):
                    calls["count"] += 1
                    if calls["count"] == 1:
                        raise RuntimeError("simulated crash before terminal write")
                    return original_terminal_put(value)

                terminal_store.put = _crash_once
                with self.assertRaises(PromotedOperationalGCSStateError):
                    restore_promoted_operational_state_from_gcs(
                        request=request, reader=reader, terminal_store=terminal_store,
                        advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                    )
                # Retry (replay): advisory/registration are already durably
                # restored (create-once, idempotent); only the terminal
                # write was missing.
                terminal_store.put = original_terminal_put
                restore = restore_promoted_operational_state_from_gcs(
                    request=request, reader=reader, terminal_store=terminal_store,
                    advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                )
                self.assertEqual(restore.terminal.terminal_id, fx.terminal.terminal_id)
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_byte_identical_full_replay_returns_the_same_terminal(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                restore1 = restore_promoted_operational_state_from_gcs(
                    request=request, reader=reader, terminal_store=terminal_store,
                    advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                )
                restore2 = restore_promoted_operational_state_from_gcs(
                    request=request, reader=reader, terminal_store=terminal_store,
                    advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                )
                self.assertEqual(restore1.terminal.terminal_id, restore2.terminal.terminal_id)
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_conflicting_pre_existing_local_terminal_fails_closed(self) -> None:
        fx_a = _Fixture(_complete_paper_buy_result)
        fx_b = _Fixture(_complete_no_trade_result)
        try:
            publication_a = fx_a.publish()
            reader_a = FakeReader(fx_a.writer)
            request_a = fx_a.restore_request(publication_a)

            tmp, terminal_store, advisory_outbox, paper_ledger = fx_a.fresh_local_stores()
            try:
                # Pre-seed a DIFFERENT terminal at the same spec_id path by
                # writing fixture B's terminal under fixture A's spec_id.
                import dataclasses

                foreign_terminal = dataclasses.replace(fx_b.terminal, spec_id=fx_a.terminal.spec_id)
                # This itself may fail identity re-derivation if spec_id
                # participates in terminal_id; either way a genuine
                # conflict must fail closed rather than being repaired.
                conflict_failed = False
                try:
                    terminal_store.put(foreign_terminal)
                except Exception:
                    conflict_failed = True
                if not conflict_failed:
                    with self.assertRaises(PromotedOperationalGCSStateError):
                        restore_promoted_operational_state_from_gcs(
                            request=request_a, reader=reader_a, terminal_store=terminal_store,
                            advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                        )
            finally:
                tmp.cleanup()
        finally:
            fx_a.close()
            fx_b.close()


class RestoreTerminalLastTests(unittest.TestCase):
    def _prepare(self, factory=_complete_paper_buy_result):
        fx = _Fixture(factory)
        publication = fx.publish()
        reader = FakeReader(fx.writer)
        request = fx.restore_request(publication)
        tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
        return fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger

    def _run(self, reader, request, terminal_store, advisory_outbox, paper_ledger):
        terminal_calls = {"count": 0}
        original_put = terminal_store.put

        def _tracked_put(value):
            terminal_calls["count"] += 1
            return original_put(value)

        terminal_store.put = _tracked_put
        error: PromotedOperationalGCSStateError | None = None
        try:
            restore_promoted_operational_state_from_gcs(
                request=request, reader=reader, terminal_store=terminal_store,
                advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
            )
        except PromotedOperationalGCSStateError as exc:
            error = exc
        return error, terminal_calls["count"]

    def _assert_blocked(self, error, terminal_calls) -> None:
        self.assertIsInstance(error, PromotedOperationalGCSStateError)
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__)
        self.assertEqual(terminal_calls, 0)

    def test_foreign_advisory_return_blocks_terminal_write(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            other = _Fixture(_complete_no_trade_result)
            foreign_advisory = other.advisory
            other.close()
            advisory_outbox.put = lambda value: foreign_advisory
            error, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self._assert_blocked(error, terminal_calls)
        finally:
            tmp.cleanup()
            fx.close()

    def test_tampered_advisory_return_blocks_terminal_write(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            tampered = fx.advisory

            def _tampered_put(value):
                object.__setattr__(tampered, "advisory_text", tampered.advisory_text + " TAMPERED")
                return tampered

            advisory_outbox.put = _tampered_put
            error, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self._assert_blocked(error, terminal_calls)
        finally:
            tmp.cleanup()
            fx.close()

    def test_wrong_type_advisory_return_blocks_terminal_write(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            advisory_outbox.put = lambda value: "not-an-advisory"
            error, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self._assert_blocked(error, terminal_calls)
        finally:
            tmp.cleanup()
            fx.close()

    def test_foreign_registration_return_blocks_terminal_write(self) -> None:
        # Same-factory fixtures are deterministic/content-identical, so a
        # genuinely foreign-but-independently-valid registration is built by
        # replacing an init field (registration_id is derived, so this
        # yields a fresh, self-consistent, differently-identified record.
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            foreign_registration = dataclasses.replace(fx.registration, quantity=fx.registration.quantity + 1)
            paper_ledger.register_value = lambda value: foreign_registration
            error, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self._assert_blocked(error, terminal_calls)
        finally:
            tmp.cleanup()
            fx.close()

    def test_tampered_registration_return_blocks_terminal_write(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            tampered = fx.registration

            def _tampered_register(value):
                object.__setattr__(tampered, "quantity", tampered.quantity + 1)
                return tampered

            paper_ledger.register_value = _tampered_register
            error, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self._assert_blocked(error, terminal_calls)
        finally:
            tmp.cleanup()
            fx.close()

    def test_wrong_type_registration_return_blocks_terminal_write(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            paper_ledger.register_value = lambda value: "not-a-registration"
            error, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self._assert_blocked(error, terminal_calls)
        finally:
            tmp.cleanup()
            fx.close()

    def test_valid_parent_pair_calls_terminal_store_put_exactly_once(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            error, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self.assertIsNone(error)
            self.assertEqual(terminal_calls, 1)
        finally:
            tmp.cleanup()
            fx.close()

    def test_wrong_terminal_return_is_rejected_without_retry(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            terminal_calls = {"count": 0}

            def _wrong_put(value):
                terminal_calls["count"] += 1
                return "not-a-terminal"

            terminal_store.put = _wrong_put
            try:
                restore_promoted_operational_state_from_gcs(
                    request=request, reader=reader, terminal_store=terminal_store,
                    advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                )
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
            self.assertEqual(terminal_calls["count"], 1)
        finally:
            tmp.cleanup()
            fx.close()


class RestoreAliasMutationTests(unittest.TestCase):
    def _prepare(self, factory=_complete_paper_buy_result):
        fx = _Fixture(factory)
        publication = fx.publish()
        reader = FakeReader(fx.writer)
        request = fx.restore_request(publication)
        tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
        return fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger

    def _run(self, reader, request, terminal_store, advisory_outbox, paper_ledger):
        terminal_calls = {"count": 0}
        original_put = terminal_store.put

        def _tracked_put(value):
            terminal_calls["count"] += 1
            return original_put(value)

        terminal_store.put = _tracked_put
        error: PromotedOperationalGCSStateError | None = None
        result = None
        try:
            result = restore_promoted_operational_state_from_gcs(
                request=request, reader=reader, terminal_store=terminal_store,
                advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
            )
        except PromotedOperationalGCSStateError as exc:
            error = exc
        return error, result, terminal_calls["count"]

    def test_advisory_outbox_same_object_mutation_blocks_terminal_write(self) -> None:
        # A faulty store mutates its exact input in place and returns that
        # same object -- expected/returned then alias, so dataclass equality
        # alone would be trivially true. Canonical-byte comparison against
        # the pre-call baseline must still catch this.
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            def _mutating_put(value):
                object.__setattr__(value, "advisory_text", value.advisory_text + " MUTATED")
                return value

            advisory_outbox.put = _mutating_put
            error, result, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self.assertIsInstance(error, PromotedOperationalGCSStateError)
            self.assertIsNone(error.__cause__)
            self.assertIsNone(error.__context__)
            self.assertEqual(terminal_calls, 0)
        finally:
            tmp.cleanup()
            fx.close()

    def test_paper_ledger_same_object_mutation_blocks_terminal_write(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            def _mutating_register(value):
                object.__setattr__(value, "quantity", value.quantity + 1)
                return value

            paper_ledger.register_value = _mutating_register
            error, result, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self.assertIsInstance(error, PromotedOperationalGCSStateError)
            self.assertIsNone(error.__cause__)
            self.assertIsNone(error.__context__)
            self.assertEqual(terminal_calls, 0)
        finally:
            tmp.cleanup()
            fx.close()

    def test_terminal_store_same_object_mutation_is_rejected_after_one_call(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            calls = {"count": 0}

            def _mutating_put(value):
                calls["count"] += 1
                object.__setattr__(value, "preparation_id", "0" * 64)
                return value

            terminal_store.put = _mutating_put
            try:
                restore_promoted_operational_state_from_gcs(
                    request=request, reader=reader, terminal_store=terminal_store,
                    advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                )
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
            self.assertEqual(calls["count"], 1)
        finally:
            tmp.cleanup()
            fx.close()

    def test_unmodified_same_object_return_is_accepted_and_calls_terminal_once(self) -> None:
        fx, reader, request, tmp, terminal_store, advisory_outbox, paper_ledger = self._prepare()
        try:
            original_advisory_put = advisory_outbox.put
            original_register = paper_ledger.register_value

            def _identity_advisory_put(value):
                original_advisory_put(value)
                return value

            def _identity_register(value):
                original_register(value)
                return value

            advisory_outbox.put = _identity_advisory_put
            paper_ledger.register_value = _identity_register
            error, result, terminal_calls = self._run(reader, request, terminal_store, advisory_outbox, paper_ledger)
            self.assertIsNone(error)
            self.assertIsNotNone(result)
            self.assertEqual(terminal_calls, 1)
        finally:
            tmp.cleanup()
            fx.close()


class RestoreAndPublicationSanitizationAuditTests(unittest.TestCase):
    def _assert_sanitized(self, exc: Exception) -> None:
        self.assertIsInstance(exc, PromotedOperationalGCSStateError)
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_malformed_date_in_restore_request_is_sanitized(self) -> None:
        try:
            PromotedOperationalGCSRestoreRequest(
                bucket=BUCKET,
                manifest_object_name=(
                    f"promoted-operational-state/v1/2026-13-99/{'a' * 64}/manifests/{'b' * 64}.json"
                ),
                generation=1, expected_sha256="c" * 64, expected_spec_id="a" * 64,
            )
            self.fail("expected PromotedOperationalGCSStateError")
        except PromotedOperationalGCSStateError as exc:
            self._assert_sanitized(exc)

    def test_tampered_published_state_object_through_artifact_is_sanitized(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            manifest = fx.publish().manifest
            real_artifact = manifest.artifacts[0]
            tampered = PublishedStateObject(
                object_name=real_artifact.published_object.object_name,
                generation=real_artifact.published_object.generation,
                byte_count=real_artifact.published_object.byte_count,
                sha256=real_artifact.published_object.sha256,
            )
            object.__setattr__(tampered, "generation", True)
            try:
                PromotedOperationalGCSArtifact(kind=real_artifact.kind, published_object=tampered)
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self._assert_sanitized(exc)
        finally:
            fx.close()

    def test_tampered_published_state_object_through_completed_publication_is_sanitized(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            publication = fx.publish()
            tampered_manifest_object = PublishedStateObject(
                object_name=publication.manifest_object.object_name,
                generation=publication.manifest_object.generation,
                byte_count=publication.manifest_object.byte_count,
                sha256=publication.manifest_object.sha256,
            )
            object.__setattr__(tampered_manifest_object, "generation", True)
            try:
                CompletedPromotedOperationalGCSPublication(
                    manifest=publication.manifest, manifest_object=tampered_manifest_object,
                )
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self._assert_sanitized(exc)
        finally:
            fx.close()

    def test_tampered_terminal_at_publication_boundary_is_sanitized(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            tampered_terminal = fx.terminal
            object.__setattr__(tampered_terminal, "preparation_id", "0" * 64)
            try:
                publish_promoted_operational_state_to_gcs(
                    terminal=tampered_terminal, bucket=BUCKET, writer=fx.writer,
                    advisory_outbox=fx.advisory_outbox, paper_ledger=fx.paper_ledger,
                )
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self._assert_sanitized(exc)
            self.assertEqual(fx.writer.calls, [])
        finally:
            fx.close()

    def test_completed_restore_sanitizes_tampered_nested_terminal(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            request = fx.restore_request(publication)
            tampered_terminal = fx.terminal
            object.__setattr__(tampered_terminal, "preparation_id", "0" * 64)
            try:
                CompletedPromotedOperationalGCSRestore(
                    request=request, manifest=publication.manifest, terminal=tampered_terminal,
                    advisory=fx.advisory, paper_registration=fx.registration,
                )
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self._assert_sanitized(exc)
        finally:
            fx.close()

    def test_completed_restore_sanitizes_tampered_nested_advisory(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            request = fx.restore_request(publication)
            tampered_advisory = fx.advisory
            object.__setattr__(tampered_advisory, "advisory_text", tampered_advisory.advisory_text + " TAMPERED")
            try:
                CompletedPromotedOperationalGCSRestore(
                    request=request, manifest=publication.manifest, terminal=fx.terminal,
                    advisory=tampered_advisory, paper_registration=fx.registration,
                )
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self._assert_sanitized(exc)
        finally:
            fx.close()

    def test_completed_restore_sanitizes_tampered_nested_registration(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            request = fx.restore_request(publication)
            tampered_registration = fx.registration
            object.__setattr__(tampered_registration, "quantity", tampered_registration.quantity + 1)
            try:
                CompletedPromotedOperationalGCSRestore(
                    request=request, manifest=publication.manifest, terminal=fx.terminal,
                    advisory=fx.advisory, paper_registration=tampered_registration,
                )
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self._assert_sanitized(exc)
        finally:
            fx.close()

    def test_completed_restore_sanitizes_foreign_bundle_verification_failure(self) -> None:
        # A same-factory second fixture would be content-identical
        # (deterministic), so use a different result shape to guarantee a
        # genuinely mismatched, independently valid advisory.
        fx_a = _Fixture(_complete_paper_buy_result)
        fx_b = _Fixture(_complete_no_trade_result)
        try:
            publication_a = fx_a.publish()
            request_a = fx_a.restore_request(publication_a)
            try:
                CompletedPromotedOperationalGCSRestore(
                    request=request_a, manifest=publication_a.manifest, terminal=fx_a.terminal,
                    advisory=fx_b.advisory, paper_registration=fx_a.registration,
                )
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self._assert_sanitized(exc)
        finally:
            fx_a.close()
            fx_b.close()


class SanitizationTests(unittest.TestCase):
    SECRET_MARKER = "SUPER-SECRET-TOKEN-MARKER-XYZ"

    def _assert_sanitized(self, exc: Exception) -> None:
        self.assertIsInstance(exc, PromotedOperationalGCSStateError)
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)
        message = str(exc)
        self.assertNotIn(self.SECRET_MARKER, message)
        self.assertNotIn(BUCKET, message)

    def test_writer_exception_containing_secret_is_sanitized(self) -> None:
        fx = _Fixture(_complete_no_trade_result)
        try:
            fx.writer.raise_error = RuntimeError(f"leaked bucket={BUCKET} token={self.SECRET_MARKER}")
            try:
                fx.publish()
                self.fail("expected PromotedOperationalGCSStateError")
            except PromotedOperationalGCSStateError as exc:
                self._assert_sanitized(exc)
        finally:
            fx.close()

    def test_reader_exception_containing_secret_is_sanitized(self) -> None:
        fx = _Fixture(_complete_paper_buy_result)
        try:
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            reader.raise_error = RuntimeError(f"leaked bucket={BUCKET} token={self.SECRET_MARKER}")
            request = fx.restore_request(publication)
            tmp, terminal_store, advisory_outbox, paper_ledger = fx.fresh_local_stores()
            try:
                try:
                    restore_promoted_operational_state_from_gcs(
                        request=request, reader=reader, terminal_store=terminal_store,
                        advisory_outbox=advisory_outbox, paper_ledger=paper_ledger,
                    )
                    self.fail("expected PromotedOperationalGCSStateError")
                except PromotedOperationalGCSStateError as exc:
                    self._assert_sanitized(exc)
            finally:
                tmp.cleanup()
        finally:
            fx.close()

    def test_malformed_bytes_do_not_leak_parsed_content(self) -> None:
        secret_bytes = f'{{"{self.SECRET_MARKER}": tru'.encode("utf-8")
        try:
            decode_promoted_operational_gcs_manifest(secret_bytes)
            self.fail("expected PromotedOperationalGCSStateError")
        except PromotedOperationalGCSStateError as exc:
            self._assert_sanitized(exc)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_forbidden_capability(self) -> None:
        source = inspect.getsource(gs_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "pathlib", "socket", "subprocess", "requests", "urllib", "httpx",
            "google", "kiteconnect", "time", "random", "threading", "asyncio",
            "sqlite3", "pickle", "shelve", "tempfile", "glob",
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
            "os.environ", "getenv(", "datetime.now(", "sleep(", "list_blobs(", "list_objects(",
            "place_order(", "generate_signal(", "run_paper_trade(", "telegram.send", "telegrambot",
            "gcloud", "subprocess.", "storage.client(",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_does_not_import_a_concrete_gcs_sdk_client(self) -> None:
        source = inspect.getsource(gs_module)
        self.assertNotIn("from google.cloud", source)
        self.assertNotIn("import google.cloud", source)

    def test_module_defines_no_scheduler_or_notification_helpers(self) -> None:
        source = inspect.getsource(gs_module)
        tree = ast.parse(source)
        defined_names = {
            node.name.lower() for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        }
        self.assertEqual(
            defined_names & {"scheduler", "notify", "send_telegram", "place_order", "deploy"}, set()
        )


if __name__ == "__main__":
    unittest.main()
