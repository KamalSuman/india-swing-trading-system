from __future__ import annotations

import ast
import hashlib
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_blob_acquisition import (
    AcquiredStateBlob,
    StateBlobAcquisitionError,
    VerifiedPipelineStateBlobs,
    acquire_verified_pipeline_state_blobs,
)
from india_swing.daily_pipeline.state_inventory import (
    MAXIMUM_FILE_BYTES,
    PipelineStateEntry,
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
    VerifiedPipelineStateControl,
)


_REPO_ROOT = Path(__file__).resolve().parent.parent
_MODULE_PATH = (
    _REPO_ROOT / "src" / "india_swing" / "daily_pipeline" / "state_blob_acquisition.py"
)
_BUCKET = "state-blob-bucket"
_RUN_ID = "a" * 64
_SESSION = date(2026, 7, 20)
_CUTOFF = datetime(2026, 7, 20, 15, 0, 0, tzinfo=timezone.utc)
_INVENTORY_GENERATION = 700
_PUBLICATION_GENERATION = 900


class _ScriptedReader:
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


def _blob_object(content: bytes, generation: int) -> PublishedStateObject:
    sha256_hash = hashlib.sha256(content).hexdigest()
    return PublishedStateObject(
        object_name=f"state/v1/blobs/{sha256_hash[:2]}/{sha256_hash}",
        generation=generation,
        byte_count=len(content),
        sha256=sha256_hash,
    )


def _build_control(
    *,
    entries: tuple[PipelineStateEntry, ...] | None = None,
    blob_objects: tuple[PublishedStateObject, ...] | None = None,
) -> tuple[VerifiedPipelineStateControl, dict[str, bytes]]:
    alpha = b"alpha-state"
    beta = b"beta-state"
    alpha_sha = hashlib.sha256(alpha).hexdigest()
    beta_sha = hashlib.sha256(beta).hexdigest()
    contents = {alpha_sha: alpha, beta_sha: beta}

    if entries is None:
        entries = (
            PipelineStateEntry(
                root_name="calendar_data",
                relative_path="a.json",
                byte_count=len(alpha),
                sha256=alpha_sha,
            ),
            PipelineStateEntry(
                root_name="calendar_data",
                relative_path="duplicate.json",
                byte_count=len(alpha),
                sha256=alpha_sha,
            ),
            PipelineStateEntry(
                root_name="daily_reports",
                relative_path="b.json",
                byte_count=len(beta),
                sha256=beta_sha,
            ),
        )

    inventory = PipelineStateInventory(
        schema_version=1,
        run_id=_RUN_ID,
        previous_run_id=None,
        market_session=_SESSION,
        cutoff=_CUTOFF,
        entries=entries,
        entry_count=len(entries),
        total_bytes=sum(item.byte_count for item in entries),
    )
    inventory_bytes = encode_pipeline_state_inventory(inventory)
    inventory_object = PublishedStateObject(
        object_name=(
            f"state/v1/inventories/{_SESSION.isoformat()}/{_RUN_ID}/"
            f"{inventory.inventory_id}.json"
        ),
        generation=_INVENTORY_GENERATION,
        byte_count=len(inventory_bytes),
        sha256=hashlib.sha256(inventory_bytes).hexdigest(),
    )

    if blob_objects is None:
        generated = (_blob_object(alpha, 101), _blob_object(beta, 202))
        blob_objects = tuple(sorted(generated, key=lambda item: item.sha256))

    manifest = PipelineStatePublicationManifest(
        schema_version=1,
        bucket=_BUCKET,
        run_id=_RUN_ID,
        previous_run_id=None,
        market_session=_SESSION,
        cutoff=_CUTOFF,
        inventory_id=inventory.inventory_id,
        inventory_object=inventory_object,
        blob_objects=blob_objects,
    )
    manifest_bytes = encode_pipeline_state_publication_manifest(manifest)
    publication_object = PublishedStateObject(
        object_name=(
            f"state/v1/publications/{_SESSION.isoformat()}/{_RUN_ID}/"
            f"{manifest.publication_id}.json"
        ),
        generation=_PUBLICATION_GENERATION,
        byte_count=len(manifest_bytes),
        sha256=hashlib.sha256(manifest_bytes).hexdigest(),
    )
    completed = CompletedPipelineStatePublication(
        manifest=manifest,
        publication_object=publication_object,
    )
    request = PinnedStatePublicationRequest(
        bucket=_BUCKET,
        publication_object_name=publication_object.object_name,
        generation=publication_object.generation,
        expected_sha256=publication_object.sha256,
        expected_run_id=_RUN_ID,
    )
    return (
        VerifiedPipelineStateControl(
            request=request,
            publication=completed,
            inventory=inventory,
        ),
        contents,
    )


def _reader_for(
    control: VerifiedPipelineStateControl,
    contents: dict[str, bytes],
) -> _ScriptedReader:
    return _ScriptedReader(
        [
            GCSObjectPayload(
                content_bytes=contents[item.sha256],
                generation=item.generation,
            )
            for item in control.publication.manifest.blob_objects
        ]
    )


class SuccessTests(unittest.TestCase):
    def test_unique_blobs_read_once_in_canonical_manifest_order(self) -> None:
        control, contents = _build_control()
        reader = _reader_for(control, contents)

        result = acquire_verified_pipeline_state_blobs(control, reader=reader)

        expected_objects = control.publication.manifest.blob_objects
        self.assertEqual(len(reader.calls), 2)
        for call, expected in zip(reader.calls, expected_objects, strict=True):
            self.assertEqual(
                call,
                {
                    "bucket": _BUCKET,
                    "object_name": expected.object_name,
                    "generation": expected.generation,
                    "maximum_bytes": MAXIMUM_FILE_BYTES,
                },
            )
        self.assertIs(type(result), VerifiedPipelineStateBlobs)
        self.assertIsNot(result.control, control)
        self.assertEqual(len(result.blobs), 2)
        self.assertEqual(
            tuple(item.published_object.sha256 for item in result.blobs),
            tuple(item.sha256 for item in expected_objects),
        )
        for item in result.blobs:
            self.assertEqual(item.content_bytes, contents[item.published_object.sha256])

    def test_empty_inventory_and_manifest_require_zero_reads(self) -> None:
        control, _ = _build_control(entries=(), blob_objects=())
        reader = _ScriptedReader([])
        result = acquire_verified_pipeline_state_blobs(control, reader=reader)
        self.assertEqual(reader.calls, [])
        self.assertEqual(result.blobs, ())


class PlanValidationTests(unittest.TestCase):
    def _assert_rejected_before_read(self, control: object) -> None:
        reader = _ScriptedReader([])
        with self.assertRaisesRegex(
            StateBlobAcquisitionError,
            "state blob acquisition (control|plan) verification failed",
        ):
            acquire_verified_pipeline_state_blobs(control, reader=reader)
        self.assertEqual(reader.calls, [])

    def test_wrong_control_type_rejected(self) -> None:
        self._assert_rejected_before_read(object())

    def test_control_subclass_rejected(self) -> None:
        class _ControlSubclass(VerifiedPipelineStateControl):
            pass

        control, _ = _build_control()
        shaped = _ControlSubclass(
            request=control.request,
            publication=control.publication,
            inventory=control.inventory,
        )
        self._assert_rejected_before_read(shaped)

    def test_manifest_missing_inventory_hash_rejected(self) -> None:
        normal, _ = _build_control()
        missing = normal.publication.manifest.blob_objects[:-1]
        control, _ = _build_control(blob_objects=missing)
        self._assert_rejected_before_read(control)

    def test_manifest_extra_hash_rejected(self) -> None:
        normal, _ = _build_control()
        extra = _blob_object(b"extra-state", 303)
        objects = tuple(
            sorted(
                normal.publication.manifest.blob_objects + (extra,),
                key=lambda item: item.sha256,
            )
        )
        control, _ = _build_control(blob_objects=objects)
        self._assert_rejected_before_read(control)

    def test_manifest_blob_byte_count_mismatch_rejected(self) -> None:
        normal, _ = _build_control()
        original = normal.publication.manifest.blob_objects[0]
        changed = PublishedStateObject(
            object_name=original.object_name,
            generation=original.generation,
            byte_count=original.byte_count + 1,
            sha256=original.sha256,
        )
        objects = tuple(
            sorted(
                (changed,) + normal.publication.manifest.blob_objects[1:],
                key=lambda item: item.sha256,
            )
        )
        control, _ = _build_control(blob_objects=objects)
        self._assert_rejected_before_read(control)

    def test_duplicate_hash_with_conflicting_inventory_sizes_rejected(self) -> None:
        alpha = b"alpha-state"
        alpha_sha = hashlib.sha256(alpha).hexdigest()
        entries = (
            PipelineStateEntry("calendar_data", "a.json", len(alpha), alpha_sha),
            PipelineStateEntry("calendar_data", "b.json", len(alpha) + 1, alpha_sha),
        )
        control, _ = _build_control(
            entries=entries,
            blob_objects=(_blob_object(alpha, 101),),
        )
        self._assert_rejected_before_read(control)

    def test_post_construction_equality_poisoned_control_rejected(self) -> None:
        class _EqualityPoison:
            def __eq__(self, other: object) -> bool:
                return True

        control, _ = _build_control()
        object.__setattr__(control.inventory, "run_id", _EqualityPoison())
        self._assert_rejected_before_read(control)


class ReadAndVerificationFailureTests(unittest.TestCase):
    def test_secret_bearing_reader_failure_is_sanitized_and_stops(self) -> None:
        control, contents = _build_control()
        expected = control.publication.manifest.blob_objects
        secret = "SECRET-BLOB-READ-FAILURE-DO-NOT-LEAK-1234"
        reader = _ScriptedReader(
            [
                GCSObjectPayload(contents[expected[0].sha256], expected[0].generation),
                RuntimeError(secret),
            ]
        )
        try:
            acquire_verified_pipeline_state_blobs(control, reader=reader)
            self.fail("expected StateBlobAcquisitionError")
        except StateBlobAcquisitionError as exc:
            self.assertEqual(str(exc), "state blob acquisition object read failed")
            self.assertNotIn(secret, str(exc))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)
        self.assertEqual(len(reader.calls), 2)

    def test_wrong_payload_type_rejected(self) -> None:
        control, _ = _build_control()
        reader = _ScriptedReader(["not-a-payload"])
        with self.assertRaisesRegex(StateBlobAcquisitionError, "object verification failed"):
            acquire_verified_pipeline_state_blobs(control, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_invalid_or_mismatched_generation_rejected(self) -> None:
        control, contents = _build_control()
        expected = control.publication.manifest.blob_objects[0]
        for generation in (None, True, str(expected.generation), expected.generation + 1):
            with self.subTest(generation=generation):
                reader = _ScriptedReader(
                    [GCSObjectPayload(contents[expected.sha256], generation)]
                )
                with self.assertRaises(StateBlobAcquisitionError):
                    acquire_verified_pipeline_state_blobs(control, reader=reader)
                self.assertEqual(len(reader.calls), 1)

    def test_non_bytes_and_empty_content_rejected(self) -> None:
        control, _ = _build_control()
        expected = control.publication.manifest.blob_objects[0]
        for content in ("not-bytes", b""):
            with self.subTest(content=content):
                reader = _ScriptedReader([GCSObjectPayload(content, expected.generation)])
                with self.assertRaises(StateBlobAcquisitionError):
                    acquire_verified_pipeline_state_blobs(control, reader=reader)
                self.assertEqual(len(reader.calls), 1)

    def test_oversized_content_rejected_without_large_allocation(self) -> None:
        control, _ = _build_control()
        expected = control.publication.manifest.blob_objects[0]
        reader = _ScriptedReader([GCSObjectPayload(b"x" * 9, expected.generation)])
        with patch(
            "india_swing.daily_pipeline.state_blob_acquisition.MAXIMUM_FILE_BYTES", 8
        ):
            with self.assertRaises(StateBlobAcquisitionError):
                acquire_verified_pipeline_state_blobs(control, reader=reader)
        self.assertEqual(len(reader.calls), 1)

    def test_byte_count_mismatch_rejected(self) -> None:
        control, contents = _build_control()
        expected = control.publication.manifest.blob_objects[0]
        content = contents[expected.sha256] + b"x"
        reader = _ScriptedReader([GCSObjectPayload(content, expected.generation)])
        with self.assertRaises(StateBlobAcquisitionError):
            acquire_verified_pipeline_state_blobs(control, reader=reader)

    def test_same_length_hash_mismatch_rejected(self) -> None:
        control, contents = _build_control()
        expected = control.publication.manifest.blob_objects[0]
        original = contents[expected.sha256]
        tampered = original[:-1] + (b"x" if original[-1:] != b"x" else b"y")
        self.assertEqual(len(tampered), len(original))
        reader = _ScriptedReader([GCSObjectPayload(tampered, expected.generation)])
        with self.assertRaises(StateBlobAcquisitionError):
            acquire_verified_pipeline_state_blobs(control, reader=reader)

    def test_base_exception_propagates_unchanged(self) -> None:
        class _Marker(BaseException):
            pass

        control, _ = _build_control()
        marker = _Marker()
        reader = _ScriptedReader([marker])
        with self.assertRaises(_Marker) as context:
            acquire_verified_pipeline_state_blobs(control, reader=reader)
        self.assertIs(context.exception, marker)


class AggregateStrictnessTests(unittest.TestCase):
    def test_blobs_must_be_exact_tuple(self) -> None:
        control, _ = _build_control(entries=(), blob_objects=())
        with self.assertRaises(StateBlobAcquisitionError):
            VerifiedPipelineStateBlobs(control=control, blobs=[])

    def test_missing_or_reordered_blob_rejected(self) -> None:
        control, contents = _build_control()
        result = acquire_verified_pipeline_state_blobs(
            control, reader=_reader_for(control, contents)
        )
        with self.assertRaises(StateBlobAcquisitionError):
            VerifiedPipelineStateBlobs(control=control, blobs=result.blobs[:-1])
        with self.assertRaises(StateBlobAcquisitionError):
            VerifiedPipelineStateBlobs(control=control, blobs=tuple(reversed(result.blobs)))

    def test_blob_subclass_rejected(self) -> None:
        class _BlobSubclass(AcquiredStateBlob):
            pass

        control, contents = _build_control()
        result = acquire_verified_pipeline_state_blobs(
            control, reader=_reader_for(control, contents)
        )
        first = result.blobs[0]
        shaped = _BlobSubclass(first.published_object, first.content_bytes)
        with self.assertRaises(StateBlobAcquisitionError):
            VerifiedPipelineStateBlobs(
                control=control,
                blobs=(shaped,) + result.blobs[1:],
            )

    def test_mutated_blob_generation_rejected(self) -> None:
        control, contents = _build_control()
        result = acquire_verified_pipeline_state_blobs(
            control, reader=_reader_for(control, contents)
        )
        object.__setattr__(result.blobs[0].published_object, "generation", 1)
        with self.assertRaises(StateBlobAcquisitionError):
            VerifiedPipelineStateBlobs(control=control, blobs=result.blobs)


class CapabilityLockTests(unittest.TestCase):
    _EXACT_ALLOWED_IMPORTS = frozenset(
        {
            (0, "__future__", "annotations", None),
            (0, "hashlib", None, None),
            (0, "dataclasses", "dataclass", None),
            (1, "acquisition", "GCSObjectPayload", None),
            (1, "acquisition", "GCSObjectReader", None),
            (1, "state_inventory", "MAXIMUM_FILE_BYTES", None),
            (1, "state_publication", "PublishedStateObject", None),
            (1, "state_publication_acquisition", "VerifiedPipelineStateControl", None),
        }
    )
    _FORBIDDEN_TOKENS = (
        "list_blobs",
        "list_objects",
        "iterdir",
        "glob",
        "latest",
        "retry",
        "fallback",
        "google",
        "storage",
        "client",
        "resolve",
        "mkdir",
        "unlink",
        "rmdir",
        "rmtree",
        "makedirs",
        "environ",
        "getenv",
        "subprocess",
        "notification",
        "llm",
        "model",
        "strategy",
        "signal",
        "broker",
        "place_order",
        "submit_order",
        "cli",
        "scheduler",
        "deploy",
    )
    _EXACT_FORBIDDEN_NAMES = frozenset(
        {"open", "write", "path", "stat", "remove", "delete", "rename", "replace", "now"}
    )

    def _module_ast(self) -> ast.Module:
        return ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))

    def test_imports_match_exact_allowlist(self) -> None:
        actual: set[tuple[int, str, str | None, str | None]] = set()
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    actual.add((0, alias.name, None, alias.asname))
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    actual.add((node.level or 0, node.module or "", alias.name, alias.asname))
        self.assertEqual(actual, self._EXACT_ALLOWED_IMPORTS)

    def test_identifiers_have_no_forbidden_capability(self) -> None:
        offenders: list[str] = []
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            if candidate in self._EXACT_FORBIDDEN_NAMES:
                offenders.append(candidate)
            for token in self._FORBIDDEN_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])

    def test_no_module_scope_call_expression(self) -> None:
        for node in self._module_ast().body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                self.fail("module-level call expression found")


if __name__ == "__main__":
    unittest.main()
