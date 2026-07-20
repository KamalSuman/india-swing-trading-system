from __future__ import annotations

import ast
import hashlib
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_inventory import (
    PipelineStateInventory,
    encode_pipeline_state_inventory,
)
from india_swing.daily_pipeline.state_publication import (
    CompletedPipelineStatePublication,
    PipelineStatePublicationManifest,
    PublishedStateObject,
    encode_pipeline_state_publication_manifest,
)
from india_swing.daily_pipeline.state_publication_acquisition import (
    PinnedStatePublicationRequest,
    StatePublicationAcquisitionError,
    VerifiedPipelineStateControl,
    acquire_verified_pipeline_state_control,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MODULE_PATH = (
    _REPO_ROOT
    / "src"
    / "india_swing"
    / "daily_pipeline"
    / "state_publication_acquisition.py"
)

_MAXIMUM_PUBLICATION_MANIFEST_BYTES = 32 * 1024 * 1024
_MAXIMUM_ENCODED_BYTES = 16 * 1024 * 1024

_SESSION = date(2026, 7, 20)
_CUTOFF = datetime(2026, 7, 20, 15, 0, 0, tzinfo=timezone.utc)
_RUN_ID = "a" * 64
_BUCKET = "control-bucket"
_INVENTORY_GENERATION = 500
_PUBLICATION_GENERATION = 999


class _ScriptedReader:
    """Fake GCSObjectReader returning one pre-scripted result per call, in
    order. Never contacts GCP. Each scripted result is either a
    GCSObjectPayload to return or a BaseException instance to raise. A
    call beyond the scripted results raises IndexError, so an
    accidental extra call fails the test loudly rather than silently
    succeeding.
    """

    def __init__(self, results: list[object]) -> None:
        self._results = list(results)
        self.calls: list[dict[str, object]] = []

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        self.calls.append(
            {
                "bucket": bucket,
                "object_name": object_name,
                "generation": generation,
                "maximum_bytes": maximum_bytes,
            }
        )
        result = self._results[len(self.calls) - 1]
        if isinstance(result, BaseException):
            raise result
        return result


def _make_inventory(
    *,
    run_id: str = _RUN_ID,
    previous_run_id: str | None = None,
    market_session: date = _SESSION,
    cutoff: datetime = _CUTOFF,
) -> PipelineStateInventory:
    return PipelineStateInventory(
        schema_version=1,
        run_id=run_id,
        previous_run_id=previous_run_id,
        market_session=market_session,
        cutoff=cutoff,
        entries=(),
        entry_count=0,
        total_bytes=0,
    )


def _inventory_object_name(market_session: date, run_id: str, inventory_id: str) -> str:
    return f"state/v1/inventories/{market_session.isoformat()}/{run_id}/{inventory_id}.json"


def _publication_object_name(market_session: date, run_id: str, publication_id: str) -> str:
    return f"state/v1/publications/{market_session.isoformat()}/{run_id}/{publication_id}.json"


def _published_object_for(content_bytes: bytes, object_name: str, generation: int) -> PublishedStateObject:
    return PublishedStateObject(
        object_name=object_name,
        generation=generation,
        byte_count=len(content_bytes),
        sha256=hashlib.sha256(content_bytes).hexdigest(),
    )


def _make_manifest(
    *,
    bucket: str = _BUCKET,
    run_id: str = _RUN_ID,
    previous_run_id: str | None = None,
    market_session: date = _SESSION,
    cutoff: datetime = _CUTOFF,
    inventory_id: str,
    inventory_object: PublishedStateObject,
    blob_objects: tuple = (),
) -> PipelineStatePublicationManifest:
    return PipelineStatePublicationManifest(
        schema_version=1,
        bucket=bucket,
        run_id=run_id,
        previous_run_id=previous_run_id,
        market_session=market_session,
        cutoff=cutoff,
        inventory_id=inventory_id,
        inventory_object=inventory_object,
        blob_objects=blob_objects,
    )


def _build_fixture(
    *,
    inventory_run_id: str = _RUN_ID,
    inventory_previous_run_id: str | None = None,
    inventory_market_session: date = _SESSION,
    inventory_cutoff: datetime = _CUTOFF,
    manifest_run_id: str = _RUN_ID,
    manifest_inventory_id_override: str | None = None,
    manifest_bucket: str = _BUCKET,
) -> tuple[PinnedStatePublicationRequest, bytes, bytes]:
    """Builds one fully consistent (unless deliberately overridden) fixture
    using only the real inventory/manifest codecs -- no filesystem, no
    tempfile. Returns (request, manifest_bytes, inventory_bytes)."""

    inventory = _make_inventory(
        run_id=inventory_run_id,
        previous_run_id=inventory_previous_run_id,
        market_session=inventory_market_session,
        cutoff=inventory_cutoff,
    )
    inventory_bytes = encode_pipeline_state_inventory(inventory)
    manifest_inventory_id = (
        manifest_inventory_id_override
        if manifest_inventory_id_override is not None
        else inventory.inventory_id
    )
    inventory_object_name = _inventory_object_name(
        _SESSION, manifest_run_id, manifest_inventory_id
    )
    inventory_object = _published_object_for(
        inventory_bytes, inventory_object_name, _INVENTORY_GENERATION
    )
    manifest = _make_manifest(
        bucket=manifest_bucket,
        run_id=manifest_run_id,
        inventory_id=manifest_inventory_id,
        inventory_object=inventory_object,
    )
    manifest_bytes = encode_pipeline_state_publication_manifest(manifest)
    publication_object_name = _publication_object_name(
        _SESSION, manifest_run_id, manifest.publication_id
    )
    request = PinnedStatePublicationRequest(
        bucket=manifest_bucket,
        publication_object_name=publication_object_name,
        generation=_PUBLICATION_GENERATION,
        expected_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        expected_run_id=manifest_run_id,
    )
    return request, manifest_bytes, inventory_bytes


def _valid_reader(manifest_bytes: bytes, inventory_bytes: bytes) -> _ScriptedReader:
    return _ScriptedReader(
        [
            GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
            GCSObjectPayload(content_bytes=inventory_bytes, generation=_INVENTORY_GENERATION),
        ]
    )


class PinnedStatePublicationRequestTests(unittest.TestCase):
    def _valid_kwargs(self) -> dict[str, object]:
        return {
            "bucket": _BUCKET,
            "publication_object_name": "state/v1/publications/2026-07-20/" + _RUN_ID + "/" + "b" * 64 + ".json",
            "generation": _PUBLICATION_GENERATION,
            "expected_sha256": "c" * 64,
            "expected_run_id": _RUN_ID,
        }

    def test_accepts_valid_request(self) -> None:
        request = PinnedStatePublicationRequest(**self._valid_kwargs())
        self.assertEqual(request.bucket, _BUCKET)

    def test_rejects_invalid_bucket(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["bucket"] = "Bad_Bucket!"
        with self.assertRaises(StatePublicationAcquisitionError):
            PinnedStatePublicationRequest(**kwargs)

    def test_rejects_empty_object_name(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["publication_object_name"] = ""
        with self.assertRaises(StatePublicationAcquisitionError):
            PinnedStatePublicationRequest(**kwargs)

    def test_rejects_non_str_object_name(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["publication_object_name"] = None
        with self.assertRaises(StatePublicationAcquisitionError):
            PinnedStatePublicationRequest(**kwargs)

    def test_rejects_noncanonical_or_wrong_run_object_name(self) -> None:
        for object_name in (
            "../escape.json",
            "state/v1/publications/2026-99-99/" + _RUN_ID + "/" + "b" * 64 + ".json",
            "state/v1/publications/2026-07-20/" + "f" * 64 + "/" + "b" * 64 + ".json",
            "state/v1/publications/2026-07-20/" + _RUN_ID + "/not-a-hash.json",
        ):
            kwargs = self._valid_kwargs()
            kwargs["publication_object_name"] = object_name
            with self.subTest(object_name=object_name):
                with self.assertRaises(StatePublicationAcquisitionError):
                    PinnedStatePublicationRequest(**kwargs)

    def test_rejects_non_positive_generation(self) -> None:
        for bad_generation in (0, -1, -100):
            kwargs = self._valid_kwargs()
            kwargs["generation"] = bad_generation
            with self.assertRaises(StatePublicationAcquisitionError):
                PinnedStatePublicationRequest(**kwargs)

    def test_rejects_non_integer_generation(self) -> None:
        for bad_generation in ("100", 100.0, True, None):
            kwargs = self._valid_kwargs()
            kwargs["generation"] = bad_generation
            with self.assertRaises(StatePublicationAcquisitionError):
                PinnedStatePublicationRequest(**kwargs)

    def test_rejects_generation_above_int64_max(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["generation"] = 9223372036854775808
        with self.assertRaises(StatePublicationAcquisitionError):
            PinnedStatePublicationRequest(**kwargs)

    def test_accepts_generation_at_int64_max(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["generation"] = 9223372036854775807
        request = PinnedStatePublicationRequest(**kwargs)
        self.assertEqual(request.generation, 9223372036854775807)

    def test_rejects_malformed_expected_sha256(self) -> None:
        for bad_sha in ("not-a-hash", "ABCDEF" * 10 + "abcd", "", "0" * 63, "0" * 65):
            kwargs = self._valid_kwargs()
            kwargs["expected_sha256"] = bad_sha
            with self.assertRaises(StatePublicationAcquisitionError):
                PinnedStatePublicationRequest(**kwargs)

    def test_rejects_malformed_expected_run_id(self) -> None:
        for bad_run_id in ("not-a-hash", "", "0" * 63, "0" * 65, None):
            kwargs = self._valid_kwargs()
            kwargs["expected_run_id"] = bad_run_id
            with self.assertRaises(StatePublicationAcquisitionError):
                PinnedStatePublicationRequest(**kwargs)


class SuccessTests(unittest.TestCase):
    def test_two_exact_reader_calls_in_order_and_fresh_verified_aggregate(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        reader = _valid_reader(manifest_bytes, inventory_bytes)

        result = acquire_verified_pipeline_state_control(request, reader=reader)

        self.assertEqual(len(reader.calls), 2)
        self.assertEqual(
            reader.calls[0],
            {
                "bucket": request.bucket,
                "object_name": request.publication_object_name,
                "generation": request.generation,
                "maximum_bytes": _MAXIMUM_PUBLICATION_MANIFEST_BYTES,
            },
        )
        # The second call's object name/generation must come from the
        # manifest's own inventory_object, not be recomputed independently.
        self.assertEqual(reader.calls[1]["generation"], _INVENTORY_GENERATION)
        self.assertEqual(reader.calls[1]["maximum_bytes"], _MAXIMUM_ENCODED_BYTES)
        self.assertEqual(reader.calls[1]["bucket"], request.bucket)

        self.assertIs(type(result), VerifiedPipelineStateControl)
        self.assertEqual(result.request.bucket, _BUCKET)
        self.assertEqual(result.publication.manifest.run_id, _RUN_ID)
        self.assertEqual(result.inventory.run_id, _RUN_ID)
        self.assertEqual(result.publication.manifest.inventory_id, result.inventory.inventory_id)

    def test_result_request_is_fresh_reconstructed_instance(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        result = acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertIsNot(result.request, request)
        self.assertEqual(result.request.bucket, request.bucket)


class RequestLevelFailureTests(unittest.TestCase):
    def test_wrong_request_type_rejected_before_any_reader_call(self) -> None:
        reader = _ScriptedReader([])
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control("not-a-request", reader=reader)
        self.assertEqual(len(reader.calls), 0)

    def test_shaped_proxy_rejected_before_any_reader_call(self) -> None:
        class _ShapedProxy:
            bucket = _BUCKET
            publication_object_name = "state/v1/publications/2026-07-20/" + _RUN_ID + "/" + "b" * 64 + ".json"
            generation = _PUBLICATION_GENERATION
            expected_sha256 = "c" * 64
            expected_run_id = _RUN_ID

        reader = _ScriptedReader([])
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(_ShapedProxy(), reader=reader)
        self.assertEqual(len(reader.calls), 0)

    def test_request_subclass_rejected_before_any_reader_call(self) -> None:
        class _ShapedRequest(PinnedStatePublicationRequest):
            pass

        request, manifest_bytes, _ = _build_fixture()
        shaped = _ShapedRequest(
            bucket=request.bucket,
            publication_object_name=request.publication_object_name,
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            expected_run_id=request.expected_run_id,
        )
        reader = _ScriptedReader([])
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(shaped, reader=reader)
        self.assertEqual(len(reader.calls), 0)

    def test_post_construction_mutated_request_rejected_before_any_reader_call(self) -> None:
        request, _, _ = _build_fixture()
        object.__setattr__(request, "bucket", "Bad_Bucket!")
        reader = _ScriptedReader([])
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 0)

    def test_equality_poisoned_request_generation_is_rejected_before_read(self) -> None:
        class _EqualityPoison:
            def __eq__(self, other: object) -> bool:
                return True

        request, _, _ = _build_fixture()
        object.__setattr__(request, "generation", _EqualityPoison())
        reader = _ScriptedReader([])
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(reader.calls, [])


class PublicationStageFailureTests(unittest.TestCase):
    def test_reader_exception_sanitized_no_inventory_read(self) -> None:
        request, _, _ = _build_fixture()
        secret = "SECRET-PUBLICATION-READ-FAILURE-DO-NOT-LEAK-1a2b"
        reader = _ScriptedReader([RuntimeError(secret)])
        try:
            acquire_verified_pipeline_state_control(request, reader=reader)
            self.fail("expected StatePublicationAcquisitionError")
        except StatePublicationAcquisitionError as exc:
            self.assertNotIn(secret, str(exc))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)
        self.assertEqual(len(reader.calls), 1)

    def test_payload_wrong_type_rejected(self) -> None:
        request, _, _ = _build_fixture()
        reader = _ScriptedReader(["not-a-payload"])
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_missing_generation_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        reader = _ScriptedReader([GCSObjectPayload(content_bytes=manifest_bytes, generation=None)])
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_bool_generation_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        reader = _ScriptedReader([GCSObjectPayload(content_bytes=manifest_bytes, generation=True)])
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_string_generation_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=manifest_bytes, generation=str(_PUBLICATION_GENERATION))]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_mismatched_generation_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION + 1)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_non_bytes_content_rejected(self) -> None:
        request, _, _ = _build_fixture()
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes="not-bytes", generation=_PUBLICATION_GENERATION)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_empty_content_rejected(self) -> None:
        request, _, _ = _build_fixture()
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=b"", generation=_PUBLICATION_GENERATION)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_oversized_content_rejected(self) -> None:
        request, _, _ = _build_fixture()
        oversized = b"a" * 9
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=oversized, generation=_PUBLICATION_GENERATION)]
        )
        with patch(
            "india_swing.daily_pipeline.state_publication_acquisition."
            "MAXIMUM_PUBLICATION_MANIFEST_BYTES",
            8,
        ):
            with self.assertRaises(StatePublicationAcquisitionError):
                acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_hash_mismatch_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        tampered = manifest_bytes + b"tampered"
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=tampered, generation=_PUBLICATION_GENERATION)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_malformed_manifest_bytes_rejected(self) -> None:
        request, _, _ = _build_fixture()
        malformed = b"not valid json manifest bytes"
        malformed_request = PinnedStatePublicationRequest(
            bucket=request.bucket,
            publication_object_name=request.publication_object_name,
            generation=request.generation,
            expected_sha256=hashlib.sha256(malformed).hexdigest(),
            expected_run_id=request.expected_run_id,
        )
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=malformed, generation=_PUBLICATION_GENERATION)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(malformed_request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_noncanonical_manifest_bytes_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        noncanonical = manifest_bytes + b"\n"
        noncanonical_request = PinnedStatePublicationRequest(
            bucket=request.bucket,
            publication_object_name=request.publication_object_name,
            generation=request.generation,
            expected_sha256=hashlib.sha256(noncanonical).hexdigest(),
            expected_run_id=request.expected_run_id,
        )
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=noncanonical, generation=_PUBLICATION_GENERATION)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(noncanonical_request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_secret_bearing_manifest_parser_failure_is_sanitized(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        secret = "SECRET-PUBLICATION-PARSER-FAILURE-DO-NOT-LEAK-5e6f"
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION)]
        )
        with patch(
            "india_swing.daily_pipeline.state_publication_acquisition."
            "parse_pipeline_state_publication_manifest",
            side_effect=RuntimeError(secret),
        ):
            try:
                acquire_verified_pipeline_state_control(request, reader=reader)
                self.fail("expected StatePublicationAcquisitionError")
            except StatePublicationAcquisitionError as exc:
                self.assertEqual(
                    str(exc),
                    "pinned state publication acquisition publication verification failed",
                )
                self.assertNotIn(secret, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
        self.assertEqual(len(reader.calls), 1)

    def test_canonical_object_name_mismatch_rejected(self) -> None:
        # The request's own publication_object_name does not match the
        # canonical name the manifest's own content implies.
        request, manifest_bytes, _ = _build_fixture()
        wrong_name_request = PinnedStatePublicationRequest(
            bucket=request.bucket,
            publication_object_name="state/v1/publications/2026-07-20/"
            + _RUN_ID
            + "/"
            + "0" * 64
            + ".json",
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            expected_run_id=request.expected_run_id,
        )
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(wrong_name_request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_bucket_mismatch_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture(manifest_bucket="one-bucket")
        mismatched_request = PinnedStatePublicationRequest(
            bucket="a-different-bucket",
            publication_object_name=request.publication_object_name,
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            expected_run_id=request.expected_run_id,
        )
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(mismatched_request, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_expected_run_mismatch_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        mismatched_run_id = "b" * 64
        mismatched_request = PinnedStatePublicationRequest(
            bucket=request.bucket,
            publication_object_name=request.publication_object_name.replace(
                f"/{request.expected_run_id}/",
                f"/{mismatched_run_id}/",
            ),
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            expected_run_id=mismatched_run_id,
        )
        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION)]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(mismatched_request, reader=reader)
        self.assertEqual(len(reader.calls), 1)


class InventoryStageFailureTests(unittest.TestCase):
    def test_reader_exception_sanitized_after_one_valid_publication_read(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        secret = "SECRET-INVENTORY-READ-FAILURE-DO-NOT-LEAK-3c4d"
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                RuntimeError(secret),
            ]
        )
        try:
            acquire_verified_pipeline_state_control(request, reader=reader)
            self.fail("expected StatePublicationAcquisitionError")
        except StatePublicationAcquisitionError as exc:
            self.assertNotIn(secret, str(exc))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_payload_wrong_type_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                "not-a-payload",
            ]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_generation_mismatch_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                GCSObjectPayload(content_bytes=inventory_bytes, generation=_INVENTORY_GENERATION + 1),
            ]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_non_bytes_content_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                GCSObjectPayload(content_bytes="not-bytes", generation=_INVENTORY_GENERATION),
            ]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_empty_content_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                GCSObjectPayload(content_bytes=b"", generation=_INVENTORY_GENERATION),
            ]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_oversized_content_rejected(self) -> None:
        request, manifest_bytes, _ = _build_fixture()
        oversized = b"a" * 9
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                GCSObjectPayload(content_bytes=oversized, generation=_INVENTORY_GENERATION),
            ]
        )
        with patch(
            "india_swing.daily_pipeline.state_publication_acquisition.MAXIMUM_ENCODED_BYTES",
            8,
        ):
            with self.assertRaises(StatePublicationAcquisitionError):
                acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_declared_byte_count_mismatch_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        truncated = inventory_bytes[:-1]
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                GCSObjectPayload(content_bytes=truncated, generation=_INVENTORY_GENERATION),
            ]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_hash_mismatch_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        # Same byte length as inventory_bytes so the byte-count check alone
        # does not also catch this: only the sha256 check should.
        tampered = inventory_bytes[:-1] + (b"0" if inventory_bytes[-1:] != b"0" else b"1")
        self.assertEqual(len(tampered), len(inventory_bytes))
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                GCSObjectPayload(content_bytes=tampered, generation=_INVENTORY_GENERATION),
            ]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_malformed_inventory_bytes_rejected(self) -> None:
        request, _, _ = _build_fixture()
        malformed = b"not valid json inventory bytes"
        _, manifest_bytes, _ = _build_fixture(
            manifest_inventory_id_override=None,
        )
        # Rebuild a request/manifest pair whose declared inventory object
        # matches this malformed payload's own byte_count/sha256 exactly,
        # so the read-stage checks pass and only the parse stage fails.
        inventory = _make_inventory()
        inventory_object_name = _inventory_object_name(_SESSION, _RUN_ID, inventory.inventory_id)
        inventory_object = _published_object_for(malformed, inventory_object_name, _INVENTORY_GENERATION)
        manifest = _make_manifest(
            inventory_id=inventory.inventory_id, inventory_object=inventory_object
        )
        real_manifest_bytes = encode_pipeline_state_publication_manifest(manifest)
        publication_object_name = _publication_object_name(_SESSION, _RUN_ID, manifest.publication_id)
        real_request = PinnedStatePublicationRequest(
            bucket=_BUCKET,
            publication_object_name=publication_object_name,
            generation=_PUBLICATION_GENERATION,
            expected_sha256=hashlib.sha256(real_manifest_bytes).hexdigest(),
            expected_run_id=_RUN_ID,
        )
        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=real_manifest_bytes, generation=_PUBLICATION_GENERATION),
                GCSObjectPayload(content_bytes=malformed, generation=_INVENTORY_GENERATION),
            ]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(real_request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_noncanonical_inventory_bytes_rejected(self) -> None:
        inventory = _make_inventory()
        canonical = encode_pipeline_state_inventory(inventory)
        noncanonical = canonical + b"\n"
        inventory_object = _published_object_for(
            noncanonical,
            _inventory_object_name(_SESSION, _RUN_ID, inventory.inventory_id),
            _INVENTORY_GENERATION,
        )
        manifest = _make_manifest(
            inventory_id=inventory.inventory_id,
            inventory_object=inventory_object,
        )
        manifest_bytes = encode_pipeline_state_publication_manifest(manifest)
        request = PinnedStatePublicationRequest(
            bucket=_BUCKET,
            publication_object_name=_publication_object_name(
                _SESSION, _RUN_ID, manifest.publication_id
            ),
            generation=_PUBLICATION_GENERATION,
            expected_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            expected_run_id=_RUN_ID,
        )
        reader = _ScriptedReader(
            [
                GCSObjectPayload(
                    content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION
                ),
                GCSObjectPayload(
                    content_bytes=noncanonical, generation=_INVENTORY_GENERATION
                ),
            ]
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_secret_bearing_inventory_parser_failure_is_sanitized(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        secret = "SECRET-INVENTORY-PARSER-FAILURE-DO-NOT-LEAK-7g8h"
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        with patch(
            "india_swing.daily_pipeline.state_publication_acquisition."
            "parse_pipeline_state_inventory",
            side_effect=RuntimeError(secret),
        ):
            try:
                acquire_verified_pipeline_state_control(request, reader=reader)
                self.fail("expected StatePublicationAcquisitionError")
            except StatePublicationAcquisitionError as exc:
                self.assertEqual(
                    str(exc),
                    "pinned state publication acquisition inventory verification failed",
                )
                self.assertNotIn(secret, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_id_mismatch_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture(
            manifest_inventory_id_override="f" * 64
        )
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_run_id_mismatch_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture(
            inventory_run_id="b" * 64
        )
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_previous_run_id_mismatch_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture(
            inventory_previous_run_id="d" * 64
        )
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_market_session_mismatch_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture(
            inventory_market_session=date(2026, 7, 21)
        )
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)

    def test_inventory_cutoff_mismatch_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture(
            inventory_cutoff=datetime(2026, 7, 20, 16, 0, 0, tzinfo=timezone.utc)
        )
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        with self.assertRaises(StatePublicationAcquisitionError):
            acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertEqual(len(reader.calls), 2)


class BaseExceptionPropagationTests(unittest.TestCase):
    def test_base_exception_from_publication_read_propagates_unchanged(self) -> None:
        request, _, _ = _build_fixture()

        class _Marker(BaseException):
            pass

        reader = _ScriptedReader([_Marker()])
        with self.assertRaises(_Marker):
            acquire_verified_pipeline_state_control(request, reader=reader)

    def test_base_exception_from_inventory_read_propagates_unchanged(self) -> None:
        request, manifest_bytes, _ = _build_fixture()

        class _Marker(BaseException):
            pass

        reader = _ScriptedReader(
            [
                GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION),
                _Marker(),
            ]
        )
        with self.assertRaises(_Marker):
            acquire_verified_pipeline_state_control(request, reader=reader)

    def test_base_exception_from_publication_parser_propagates_unchanged(self) -> None:
        request, manifest_bytes, _ = _build_fixture()

        class _Marker(BaseException):
            pass

        reader = _ScriptedReader(
            [GCSObjectPayload(content_bytes=manifest_bytes, generation=_PUBLICATION_GENERATION)]
        )
        marker = _Marker()
        with patch(
            "india_swing.daily_pipeline.state_publication_acquisition."
            "parse_pipeline_state_publication_manifest",
            side_effect=marker,
        ):
            with self.assertRaises(_Marker) as context:
                acquire_verified_pipeline_state_control(request, reader=reader)
        self.assertIs(context.exception, marker)

    def test_base_exception_from_inventory_parser_propagates_unchanged(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()

        class _Marker(BaseException):
            pass

        marker = _Marker()
        with patch(
            "india_swing.daily_pipeline.state_publication_acquisition."
            "parse_pipeline_state_inventory",
            side_effect=marker,
        ):
            with self.assertRaises(_Marker) as context:
                acquire_verified_pipeline_state_control(
                    request, reader=_valid_reader(manifest_bytes, inventory_bytes)
                )
        self.assertIs(context.exception, marker)


class VerifiedPipelineStateControlTests(unittest.TestCase):
    def test_wrong_publication_type_rejected(self) -> None:
        request, _, _ = _build_fixture()
        inventory = _make_inventory()
        with self.assertRaises(StatePublicationAcquisitionError):
            VerifiedPipelineStateControl(
                request=request, publication="not-a-publication", inventory=inventory
            )

    def test_wrong_inventory_type_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        result = acquire_verified_pipeline_state_control(request, reader=reader)
        with self.assertRaises(StatePublicationAcquisitionError):
            VerifiedPipelineStateControl(
                request=request, publication=result.publication, inventory="not-an-inventory"
            )

    def test_post_construction_mutated_publication_object_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        result = acquire_verified_pipeline_state_control(request, reader=reader)
        mutated_publication = CompletedPipelineStatePublication(
            manifest=result.publication.manifest,
            publication_object=result.publication.publication_object,
        )
        object.__setattr__(mutated_publication.publication_object, "generation", 1)
        try:
            VerifiedPipelineStateControl(
                request=request, publication=mutated_publication, inventory=result.inventory
            )
            self.fail("expected StatePublicationAcquisitionError")
        except StatePublicationAcquisitionError as exc:
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)

    def test_publication_object_name_must_remain_bound_to_request(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        result = acquire_verified_pipeline_state_control(
            request, reader=_valid_reader(manifest_bytes, inventory_bytes)
        )
        different_request = PinnedStatePublicationRequest(
            bucket=request.bucket,
            publication_object_name="state/v1/publications/2026-07-20/"
            + _RUN_ID
            + "/"
            + "f" * 64
            + ".json",
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            expected_run_id=request.expected_run_id,
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            VerifiedPipelineStateControl(
                request=different_request,
                publication=result.publication,
                inventory=result.inventory,
            )

    def test_publication_object_hash_must_remain_bound_to_request(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        result = acquire_verified_pipeline_state_control(
            request, reader=_valid_reader(manifest_bytes, inventory_bytes)
        )
        different_request = PinnedStatePublicationRequest(
            bucket=request.bucket,
            publication_object_name=request.publication_object_name,
            generation=request.generation,
            expected_sha256="f" * 64,
            expected_run_id=request.expected_run_id,
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            VerifiedPipelineStateControl(
                request=different_request,
                publication=result.publication,
                inventory=result.inventory,
            )

    def test_inventory_is_defensively_reconstructed(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        result = acquire_verified_pipeline_state_control(
            request, reader=_valid_reader(manifest_bytes, inventory_bytes)
        )
        second = VerifiedPipelineStateControl(
            request=request,
            publication=result.publication,
            inventory=result.inventory,
        )
        self.assertIsNot(second.inventory, result.inventory)
        self.assertEqual(second.inventory.inventory_id, result.inventory.inventory_id)

    def test_equality_poisoned_inventory_run_id_is_rejected(self) -> None:
        class _EqualityPoison:
            def __eq__(self, other: object) -> bool:
                return True

        request, manifest_bytes, inventory_bytes = _build_fixture()
        result = acquire_verified_pipeline_state_control(
            request, reader=_valid_reader(manifest_bytes, inventory_bytes)
        )
        object.__setattr__(result.inventory, "run_id", _EqualityPoison())
        with self.assertRaises(StatePublicationAcquisitionError):
            VerifiedPipelineStateControl(
                request=request,
                publication=result.publication,
                inventory=result.inventory,
            )

    def test_manifest_bucket_mismatch_with_request_rejected(self) -> None:
        request, manifest_bytes, inventory_bytes = _build_fixture()
        reader = _valid_reader(manifest_bytes, inventory_bytes)
        result = acquire_verified_pipeline_state_control(request, reader=reader)
        different_bucket_request = PinnedStatePublicationRequest(
            bucket="a-completely-different-bucket",
            publication_object_name=request.publication_object_name,
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            expected_run_id=request.expected_run_id,
        )
        with self.assertRaises(StatePublicationAcquisitionError):
            VerifiedPipelineStateControl(
                request=different_bucket_request,
                publication=result.publication,
                inventory=result.inventory,
            )


class CapabilityLockTests(unittest.TestCase):
    _EXACT_ALLOWED_IMPORTS = frozenset(
        {
            (0, "__future__", "annotations", None),
            (0, "hashlib", None, None),
            (0, "dataclasses", "dataclass", None),
            (0, "datetime", "date", None),
            (1, "acquisition", "GCSObjectPayload", None),
            (1, "acquisition", "GCSObjectReader", None),
            (1, "acquisition", "_MAXIMUM_GENERATION", None),
            (1, "state_inventory", "MAXIMUM_ENCODED_BYTES", None),
            (1, "state_inventory", "PipelineStateInventory", None),
            (1, "state_inventory", "parse_pipeline_state_inventory", None),
            (1, "state_publication", "MAXIMUM_PUBLICATION_MANIFEST_BYTES", None),
            (1, "state_publication", "CompletedPipelineStatePublication", None),
            (1, "state_publication", "PipelineStatePublicationManifest", None),
            (1, "state_publication", "PublishedStateObject", None),
            (1, "state_publication", "_validate_bucket", None),
            (1, "state_publication", "parse_pipeline_state_publication_manifest", None),
        }
    )
    _FORBIDDEN_TOKENS = (
        "list_blobs",
        "list_buckets",
        "get_bucket",
        "latest",
        "delete",
        "rewrite",
        "compose",
        "copy_blob",
        "overwrite",
        "broker",
        "place_order",
        "submit_order",
        "notification",
        "scheduler",
        "socket",
        "subprocess",
        "tempfile",
        "shutil",
        "environ",
        "getenv",
        "requests",
        "urllib",
        "unlink",
        "remove",
        "rename",
        "rmdir",
        "rmtree",
        "mkstemp",
        "mkdtemp",
        "mkdir",
        "makedirs",
        "popen",
        "system",
        "eval",
        "storage",
        "client",
        "signal",
        "confidence",
        "alert",
        "hydrate",
        "deploy",
        "capital",
        "strategy",
    )
    _EXACT_FORBIDDEN_NAMES = frozenset({"now", "open", "exec", "path", "resolve", "stat"})

    def _module_ast(self) -> ast.Module:
        source = _MODULE_PATH.read_text(encoding="utf-8")
        return ast.parse(source)

    def test_imports_match_an_exact_allowlist(self) -> None:
        tree = self._module_ast()
        actual: set[tuple[int, str, str | None, str | None]] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    actual.add((0, alias.name, None, alias.asname))
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    actual.add((node.level or 0, node.module or "", alias.name, alias.asname))
        self.assertEqual(actual, self._EXACT_ALLOWED_IMPORTS)

    def test_identifiers_carry_no_disallowed_capability_token(self) -> None:
        tree = self._module_ast()
        for node in ast.walk(tree):
            name = None
            if isinstance(node, ast.Name):
                name = node.id
            elif isinstance(node, ast.Attribute):
                name = node.attr
            if name is None:
                continue
            lowered = name.lower()
            if lowered in self._EXACT_FORBIDDEN_NAMES:
                self.fail(f"forbidden exact identifier used: {name}")
            for token in self._FORBIDDEN_TOKENS:
                self.assertNotIn(token, lowered, name)

    def test_no_module_scope_call_expression(self) -> None:
        tree = self._module_ast()
        for node in tree.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                self.fail("module-level call expression found")

    def test_no_storage_client_or_filesystem_construction_at_all(self) -> None:
        source = _MODULE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("google", source)
        self.assertNotIn("Client(", source)
        self.assertNotIn("import os", source)
        self.assertNotIn("import pathlib", source)
        self.assertNotIn("from pathlib", source)


if __name__ == "__main__":
    unittest.main()
