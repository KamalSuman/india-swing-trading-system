from __future__ import annotations

import ast
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import india_swing.daily_pipeline.pinned_gcs_state_restore_file_boundary as boundary_module
from india_swing.daily_pipeline.pinned_gcs_state_restore_file_boundary import (
    PinnedGCSStateRestoreFileBoundaryError,
    load_pinned_gcs_state_restore_spec_file,
    restore_pipeline_state_from_pinned_gcs_spec_file,
)
from india_swing.daily_pipeline.pinned_gcs_state_restoration_service import (
    CompletedPinnedGCSStateRestore,
    PinnedGCSStateRestorationServiceError,
)
from india_swing.daily_pipeline.pinned_gcs_state_restore_spec import (
    MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES,
    PinnedGCSStateRestoreSpec,
    encode_pinned_gcs_state_restore_spec,
)

from tests.test_pinned_gcs_state_restoration_service import _fixture


_MODULE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "india_swing"
    / "daily_pipeline"
    / "pinned_gcs_state_restore_file_boundary.py"
)


class BoundaryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.snapshots = self.root / "snapshots"
        self.snapshots.mkdir()
        self.request, self.reader, _ = _fixture()
        self.destination = self.snapshots / self.request.expected_run_id
        self.spec = PinnedGCSStateRestoreSpec(
            schema_version=1,
            publication_request=self.request,
            destination=self.destination,
        )
        self.spec_bytes = encode_pinned_gcs_state_restore_spec(self.spec)
        self.spec_path = self.root / "restore-spec.json"
        self.spec_path.write_bytes(self.spec_bytes)

    def tearDown(self) -> None:
        self.temporary.cleanup()


class LoadTests(BoundaryTestCase):
    def test_loads_one_stable_canonical_file_and_reconstructs(self) -> None:
        loaded = load_pinned_gcs_state_restore_spec_file(self.spec_path)
        self.assertIs(type(loaded), PinnedGCSStateRestoreSpec)
        self.assertIsNot(loaded, self.spec)
        self.assertIsNot(loaded.publication_request, self.request)
        self.assertEqual(loaded.destination, self.destination)

    def test_stable_reader_receives_exact_path_and_limit(self) -> None:
        with patch.object(
            boundary_module,
            "read_stable_regular_file",
            return_value=self.spec_bytes,
        ) as read:
            loaded = load_pinned_gcs_state_restore_spec_file(self.spec_path)
        self.assertEqual(loaded.destination, self.destination)
        read.assert_called_once_with(
            self.spec_path,
            maximum_bytes=MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES,
        )

    def test_rejects_wrong_relative_missing_directory_empty_and_oversized_inputs(self) -> None:
        empty = self.root / "empty.json"
        empty.write_bytes(b"")
        oversized = self.root / "oversized.json"
        oversized.write_bytes(
            b" " * (MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES + 1)
        )
        missing = self.root / "missing.json"
        cases: tuple[object, ...] = (
            str(self.spec_path),
            Path("restore-spec.json"),
            missing,
            self.root,
            empty,
            oversized,
        )
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    PinnedGCSStateRestoreFileBoundaryError,
                    "^pinned state restoration spec file could not be loaded$",
                ):
                    load_pinned_gcs_state_restore_spec_file(value)  # type: ignore[arg-type]

    def test_rejects_symlink_without_following_when_available(self) -> None:
        symlink = self.root / "restore-link.json"
        try:
            os.symlink(self.spec_path, symlink)
        except OSError:
            self.skipTest("symlink creation is unavailable")
        with self.assertRaises(PinnedGCSStateRestoreFileBoundaryError):
            load_pinned_gcs_state_restore_spec_file(symlink)

    def test_rejects_noncanonical_file_content(self) -> None:
        self.spec_path.write_bytes(self.spec_bytes[:-1])
        with self.assertRaises(PinnedGCSStateRestoreFileBoundaryError):
            load_pinned_gcs_state_restore_spec_file(self.spec_path)

    def test_rejects_parser_subclass_or_mutated_result(self) -> None:
        class SubclassedSpec(PinnedGCSStateRestoreSpec):
            pass

        subclassed = SubclassedSpec(
            schema_version=1,
            publication_request=self.request,
            destination=self.destination,
        )
        with patch.object(
            boundary_module,
            "parse_pinned_gcs_state_restore_spec",
            return_value=subclassed,
        ):
            with self.assertRaisesRegex(
                PinnedGCSStateRestoreFileBoundaryError,
                "schema verification failed$",
            ):
                load_pinned_gcs_state_restore_spec_file(self.spec_path)

        mutated = PinnedGCSStateRestoreSpec(
            schema_version=1,
            publication_request=self.request,
            destination=self.destination,
        )
        object.__setattr__(mutated, "schema_version", True)
        with patch.object(
            boundary_module,
            "parse_pinned_gcs_state_restore_spec",
            return_value=mutated,
        ):
            with self.assertRaises(PinnedGCSStateRestoreFileBoundaryError):
                load_pinned_gcs_state_restore_spec_file(self.spec_path)

    def test_load_failure_is_sanitized_without_nested_context(self) -> None:
        secret = "secret-stable-read-failure"
        with patch.object(
            boundary_module,
            "read_stable_regular_file",
            side_effect=RuntimeError(secret),
        ):
            try:
                load_pinned_gcs_state_restore_spec_file(self.spec_path)
                self.fail("expected boundary error")
            except PinnedGCSStateRestoreFileBoundaryError as caught:
                self.assertNotIn(secret, str(caught))
                self.assertIsNone(caught.__cause__)
                self.assertIsNone(caught.__context__)


class DelegationTests(BoundaryTestCase):
    def test_end_to_end_file_boundary_restores_snapshot(self) -> None:
        result = restore_pipeline_state_from_pinned_gcs_spec_file(
            self.spec_path,
            reader=self.reader,
        )
        self.assertIs(type(result), CompletedPinnedGCSStateRestore)
        self.assertEqual(result.destination, self.destination)
        self.assertTrue(self.destination.is_dir())

    def test_delegates_once_with_reconstructed_values_and_reader_identity(self) -> None:
        sentinel = object()
        with patch.object(
            boundary_module,
            "restore_pipeline_state_from_pinned_gcs",
            return_value=sentinel,
        ) as service:
            result = restore_pipeline_state_from_pinned_gcs_spec_file(
                self.spec_path,
                reader=self.reader,
            )
        self.assertIs(result, sentinel)
        service.assert_called_once()
        args, kwargs = service.call_args
        self.assertEqual(args, (self.spec.publication_request,))
        self.assertIsNot(args[0], self.spec.publication_request)
        self.assertIs(kwargs["reader"], self.reader)
        self.assertEqual(kwargs["destination"], self.destination)

    def test_service_error_propagates_unchanged(self) -> None:
        expected = PinnedGCSStateRestorationServiceError("service sentinel")
        with patch.object(
            boundary_module,
            "restore_pipeline_state_from_pinned_gcs",
            side_effect=expected,
        ):
            with self.assertRaises(PinnedGCSStateRestorationServiceError) as caught:
                restore_pipeline_state_from_pinned_gcs_spec_file(
                    self.spec_path,
                    reader=self.reader,
                )
        self.assertIs(caught.exception, expected)

    def test_base_exception_from_loader_or_service_propagates(self) -> None:
        with patch.object(
            boundary_module,
            "read_stable_regular_file",
            side_effect=KeyboardInterrupt,
        ):
            with self.assertRaises(KeyboardInterrupt):
                restore_pipeline_state_from_pinned_gcs_spec_file(
                    self.spec_path,
                    reader=self.reader,
                )
        with patch.object(
            boundary_module,
            "restore_pipeline_state_from_pinned_gcs",
            side_effect=KeyboardInterrupt,
        ):
            with self.assertRaises(KeyboardInterrupt):
                restore_pipeline_state_from_pinned_gcs_spec_file(
                    self.spec_path,
                    reader=self.reader,
                )


class CapabilityLockTests(unittest.TestCase):
    _EXACT_ALLOWED_IMPORTS = frozenset(
        {
            (0, "__future__", "annotations", None),
            (0, "pathlib", "Path", None),
            (0, "india_swing._filesystem", "read_stable_regular_file", None),
            (1, "acquisition", "GCSObjectReader", None),
            (
                1,
                "pinned_gcs_state_restoration_service",
                "CompletedPinnedGCSStateRestore",
                None,
            ),
            (
                1,
                "pinned_gcs_state_restoration_service",
                "restore_pipeline_state_from_pinned_gcs",
                None,
            ),
            (
                1,
                "pinned_gcs_state_restore_spec",
                "MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES",
                None,
            ),
            (
                1,
                "pinned_gcs_state_restore_spec",
                "PinnedGCSStateRestoreSpec",
                None,
            ),
            (
                1,
                "pinned_gcs_state_restore_spec",
                "parse_pinned_gcs_state_restore_spec",
                None,
            ),
        }
    )

    def _module_ast(self) -> ast.Module:
        return ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))

    def test_imports_match_exact_boundary_allowlist(self) -> None:
        actual: set[tuple[int, str, str | None, str | None]] = set()
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    actual.add((0, alias.name, None, alias.asname))
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    actual.add((node.level or 0, node.module or "", alias.name, alias.asname))
        self.assertEqual(actual, self._EXACT_ALLOWED_IMPORTS)

    def test_no_direct_open_stat_listing_latest_retry_or_mutation(self) -> None:
        forbidden = frozenset(
            {
                "open",
                "lstat",
                "stat",
                "iterdir",
                "glob",
                "mkdir",
                "write",
                "unlink",
                "rename",
                "replace",
                "rmtree",
                "latest",
                "retry",
                "list_blobs",
                "getenv",
            }
        )
        offenders: list[str] = []
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            if candidate in forbidden:
                offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
