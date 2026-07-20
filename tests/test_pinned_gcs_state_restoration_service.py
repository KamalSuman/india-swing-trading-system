from __future__ import annotations

import ast
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import india_swing.daily_pipeline.pinned_gcs_state_restoration_service as service_module
from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.pinned_gcs_state_restoration_service import (
    CompletedPinnedGCSStateRestore,
    PinnedGCSStateRestorationServiceError,
    restore_pipeline_state_from_pinned_gcs,
)
from india_swing.daily_pipeline.state_inventory import (
    MAXIMUM_ENCODED_BYTES,
    MAXIMUM_FILE_BYTES,
    encode_pipeline_state_inventory,
)
from india_swing.daily_pipeline.state_publication import (
    MAXIMUM_PUBLICATION_MANIFEST_BYTES,
    encode_pipeline_state_publication_manifest,
)
from india_swing.daily_pipeline.state_publication_acquisition import (
    PinnedStatePublicationRequest,
)
from india_swing.daily_pipeline.state_restoration import (
    CompletedPipelineStateRestore,
)

from tests.test_state_blob_acquisition import (
    _ScriptedReader,
    _build_control,
)


_MODULE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "india_swing"
    / "daily_pipeline"
    / "pinned_gcs_state_restoration_service.py"
)


def _fixture() -> tuple[
    PinnedStatePublicationRequest,
    _ScriptedReader,
    tuple[object, ...],
]:
    control, contents = _build_control()
    manifest = control.publication.manifest
    payloads: list[object] = [
        GCSObjectPayload(
            content_bytes=encode_pipeline_state_publication_manifest(manifest),
            generation=control.request.generation,
        ),
        GCSObjectPayload(
            content_bytes=encode_pipeline_state_inventory(control.inventory),
            generation=manifest.inventory_object.generation,
        ),
    ]
    payloads.extend(
        GCSObjectPayload(
            content_bytes=contents[item.sha256],
            generation=item.generation,
        )
        for item in manifest.blob_objects
    )
    return control.request, _ScriptedReader(payloads), manifest.blob_objects


class ServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.parent = Path(self.temporary.name).resolve()
        self.request, self.reader, self.blob_objects = _fixture()
        self.destination = self.parent / self.request.expected_run_id

    def tearDown(self) -> None:
        self.temporary.cleanup()


class SuccessTests(ServiceTestCase):
    def test_composes_exact_reads_and_atomic_restore_in_order(self) -> None:
        result = restore_pipeline_state_from_pinned_gcs(
            self.request,
            reader=self.reader,
            destination=self.destination,
        )

        self.assertIs(type(result), CompletedPinnedGCSStateRestore)
        self.assertIsNot(result.request, self.request)
        self.assertEqual(result.destination, self.destination)
        self.assertEqual(result.restoration.snapshot_root, self.destination)
        self.assertEqual(
            len(self.reader.calls),
            2 + len(self.blob_objects),
        )
        self.assertEqual(
            self.reader.calls[0],
            {
                "bucket": self.request.bucket,
                "object_name": self.request.publication_object_name,
                "generation": self.request.generation,
                "maximum_bytes": MAXIMUM_PUBLICATION_MANIFEST_BYTES,
            },
        )
        inventory_object = result.state.acquired_blobs.control.publication.manifest.inventory_object
        self.assertEqual(
            self.reader.calls[1],
            {
                "bucket": self.request.bucket,
                "object_name": inventory_object.object_name,
                "generation": inventory_object.generation,
                "maximum_bytes": MAXIMUM_ENCODED_BYTES,
            },
        )
        for call, expected in zip(
            self.reader.calls[2:],
            self.blob_objects,
            strict=True,
        ):
            self.assertEqual(
                call,
                {
                    "bucket": self.request.bucket,
                    "object_name": expected.object_name,
                    "generation": expected.generation,
                    "maximum_bytes": MAXIMUM_FILE_BYTES,
                },
            )
        for item in result.state.entries:
            entry = item.inventory_entry
            restored = getattr(
                result.restoration.roots,
                entry.root_name,
            ).joinpath(*entry.relative_path.split("/"))
            self.assertEqual(restored.read_bytes(), item.content_bytes)


class PreflightTests(ServiceTestCase):
    def test_rejects_request_subclass_before_reader_or_filesystem(self) -> None:
        class SubclassedRequest(PinnedStatePublicationRequest):
            pass

        request = SubclassedRequest(
            bucket=self.request.bucket,
            publication_object_name=self.request.publication_object_name,
            generation=self.request.generation,
            expected_sha256=self.request.expected_sha256,
            expected_run_id=self.request.expected_run_id,
        )
        with self.assertRaisesRegex(
            PinnedGCSStateRestorationServiceError,
            "^pinned state restoration service input verification failed$",
        ):
            restore_pipeline_state_from_pinned_gcs(
                request,
                reader=self.reader,
                destination=self.destination,
            )
        self.assertEqual(self.reader.calls, [])
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_rejects_wrong_destination_before_reader_or_filesystem(self) -> None:
        with self.assertRaises(PinnedGCSStateRestorationServiceError):
            restore_pipeline_state_from_pinned_gcs(
                self.request,
                reader=self.reader,
                destination=self.parent / ("f" * 64),
            )
        self.assertEqual(self.reader.calls, [])
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_rejects_relative_destination_before_reader(self) -> None:
        with self.assertRaises(PinnedGCSStateRestorationServiceError):
            restore_pipeline_state_from_pinned_gcs(
                self.request,
                reader=self.reader,
                destination=Path(self.request.expected_run_id),
            )
        self.assertEqual(self.reader.calls, [])


class StageFailureTests(ServiceTestCase):
    def test_control_failure_stops_before_blob_reads_and_restore(self) -> None:
        reader = _ScriptedReader([RuntimeError("secret control failure")])
        with self.assertRaisesRegex(
            PinnedGCSStateRestorationServiceError,
            "^pinned state restoration service control acquisition failed$",
        ) as caught:
            restore_pipeline_state_from_pinned_gcs(
                self.request,
                reader=reader,
                destination=self.destination,
            )
        self.assertEqual(len(reader.calls), 1)
        self.assertFalse(self.destination.exists())
        self.assertNotIn("secret", str(caught.exception))

    def test_blob_failure_stops_before_hydration_and_restore(self) -> None:
        _, valid_reader, _ = _fixture()
        reader = _ScriptedReader(valid_reader._results[:2] + [RuntimeError("secret blob")])
        with self.assertRaisesRegex(
            PinnedGCSStateRestorationServiceError,
            "^pinned state restoration service blob acquisition failed$",
        ):
            restore_pipeline_state_from_pinned_gcs(
                self.request,
                reader=reader,
                destination=self.destination,
            )
        self.assertEqual(len(reader.calls), 3)
        self.assertFalse(self.destination.exists())

    def test_hydration_failure_stops_before_restore(self) -> None:
        with patch.object(
            service_module,
            "hydrate_verified_pipeline_state",
            side_effect=RuntimeError("secret hydration"),
        ):
            with self.assertRaisesRegex(
                PinnedGCSStateRestorationServiceError,
                "^pinned state restoration service hydration failed$",
            ):
                restore_pipeline_state_from_pinned_gcs(
                    self.request,
                    reader=self.reader,
                    destination=self.destination,
                )
        self.assertEqual(len(self.reader.calls), 2 + len(self.blob_objects))
        self.assertFalse(self.destination.exists())

    def test_restoration_failure_returns_no_completion(self) -> None:
        with patch.object(
            service_module,
            "restore_verified_pipeline_state",
            side_effect=RuntimeError("secret restoration"),
        ):
            with self.assertRaisesRegex(
                PinnedGCSStateRestorationServiceError,
                "^pinned state restoration service restoration failed$",
            ):
                restore_pipeline_state_from_pinned_gcs(
                    self.request,
                    reader=self.reader,
                    destination=self.destination,
                )
        self.assertFalse(self.destination.exists())

    def test_aggregate_failure_does_not_delete_completed_snapshot(self) -> None:
        with patch.object(
            service_module,
            "CompletedPinnedGCSStateRestore",
            side_effect=RuntimeError("secret aggregate"),
        ):
            with self.assertRaisesRegex(
                PinnedGCSStateRestorationServiceError,
                "^pinned state restoration service aggregate verification failed$",
            ):
                restore_pipeline_state_from_pinned_gcs(
                    self.request,
                    reader=self.reader,
                    destination=self.destination,
                )
        self.assertTrue(self.destination.is_dir())
        self.assertTrue((self.destination / "calendar_data" / "a.json").is_file())

    def test_base_exception_propagates(self) -> None:
        with patch.object(
            service_module,
            "acquire_verified_pipeline_state_control",
            side_effect=KeyboardInterrupt,
        ):
            with self.assertRaises(KeyboardInterrupt):
                restore_pipeline_state_from_pinned_gcs(
                    self.request,
                    reader=self.reader,
                    destination=self.destination,
                )


class AggregateStrictnessTests(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.completed = restore_pipeline_state_from_pinned_gcs(
            self.request,
            reader=self.reader,
            destination=self.destination,
        )

    def test_reconstructs_all_nested_values(self) -> None:
        rebuilt = CompletedPinnedGCSStateRestore(
            request=self.completed.request,
            destination=self.completed.destination,
            state=self.completed.state,
            restoration=self.completed.restoration,
        )
        self.assertIsNot(rebuilt.request, self.completed.request)
        self.assertIsNot(rebuilt.state, self.completed.state)
        self.assertIsNot(rebuilt.restoration, self.completed.restoration)

    def test_rejects_mismatched_destination(self) -> None:
        other_parent = self.parent / "other"
        other_parent.mkdir()
        with self.assertRaisesRegex(
            PinnedGCSStateRestorationServiceError,
            "^pinned state restoration service aggregate verification failed$",
        ):
            CompletedPinnedGCSStateRestore(
                request=self.completed.request,
                destination=other_parent / self.request.expected_run_id,
                state=self.completed.state,
                restoration=self.completed.restoration,
            )

    def test_rejects_mismatched_request(self) -> None:
        mismatched = PinnedStatePublicationRequest(
            bucket=self.request.bucket,
            publication_object_name=self.request.publication_object_name,
            generation=self.request.generation,
            expected_sha256="f" * 64,
            expected_run_id=self.request.expected_run_id,
        )
        with self.assertRaises(PinnedGCSStateRestorationServiceError):
            CompletedPinnedGCSStateRestore(
                request=mismatched,
                destination=self.completed.destination,
                state=self.completed.state,
                restoration=self.completed.restoration,
            )

    def test_rejects_mutated_state(self) -> None:
        object.__setattr__(self.completed.state, "entries", ())
        with self.assertRaises(PinnedGCSStateRestorationServiceError):
            CompletedPinnedGCSStateRestore(
                request=self.completed.request,
                destination=self.completed.destination,
                state=self.completed.state,
                restoration=self.completed.restoration,
            )

    def test_rejects_restoration_subclass(self) -> None:
        class SubclassedRestore(CompletedPipelineStateRestore):
            pass

        original = self.completed.restoration
        subclassed = SubclassedRestore(
            snapshot_root=original.snapshot_root,
            roots=original.roots,
            run_id=original.run_id,
            inventory_id=original.inventory_id,
        )
        with self.assertRaises(PinnedGCSStateRestorationServiceError):
            CompletedPinnedGCSStateRestore(
                request=self.completed.request,
                destination=self.completed.destination,
                state=self.completed.state,
                restoration=subclassed,
            )


class CapabilityLockTests(unittest.TestCase):
    _EXACT_ALLOWED_IMPORTS = frozenset(
        {
            (0, "__future__", "annotations", None),
            (0, "dataclasses", "dataclass", None),
            (0, "pathlib", "Path", None),
            (1, "acquisition", "GCSObjectReader", None),
            (1, "state_blob_acquisition", "VerifiedPipelineStateBlobs", None),
            (1, "state_blob_acquisition", "acquire_verified_pipeline_state_blobs", None),
            (1, "state_hydration", "VerifiedHydratedPipelineState", None),
            (1, "state_hydration", "hydrate_verified_pipeline_state", None),
            (1, "state_publication_acquisition", "PinnedStatePublicationRequest", None),
            (1, "state_publication_acquisition", "VerifiedPipelineStateControl", None),
            (1, "state_publication_acquisition", "acquire_verified_pipeline_state_control", None),
            (1, "state_restoration", "CompletedPipelineStateRestore", None),
            (1, "state_restoration", "restore_verified_pipeline_state", None),
        }
    )

    def _module_ast(self) -> ast.Module:
        return ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))

    def test_imports_match_exact_composition_allowlist(self) -> None:
        actual: set[tuple[int, str, str | None, str | None]] = set()
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    actual.add((0, alias.name, None, alias.asname))
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    actual.add((node.level or 0, node.module or "", alias.name, alias.asname))
        self.assertEqual(actual, self._EXACT_ALLOWED_IMPORTS)

    def test_has_no_client_latest_retry_environment_or_direct_mutation(self) -> None:
        forbidden = (
            "client",
            "latest",
            "retry",
            "fallback",
            "environ",
            "getenv",
            "list_blobs",
            "mkdir",
            "rename",
            "replace",
            "rmtree",
            "unlink",
            "write",
            "open",
        )
        offenders: list[str] = []
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            if any(token in candidate for token in forbidden):
                offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
