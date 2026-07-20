from __future__ import annotations

import ast
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import india_swing.daily_pipeline.state_restoration as restoration_module
from india_swing.daily_pipeline.state_hydration import (
    hydrate_verified_pipeline_state,
)
from india_swing.daily_pipeline.state_inventory import ROOT_NAMES
from india_swing.daily_pipeline.state_restoration import (
    CompletedPipelineStateRestore,
    PipelineStateRestorationError,
    restore_verified_pipeline_state,
)

from tests.test_state_hydration import _acquired


_MODULE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "india_swing"
    / "daily_pipeline"
    / "state_restoration.py"
)


class RestorationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.parent = Path(self.temporary.name).resolve()
        acquired, _ = _acquired()
        self.state = hydrate_verified_pipeline_state(acquired)
        self.run_id = acquired.control.inventory.run_id
        self.destination = self.parent / self.run_id

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def assert_no_staging_tree(self) -> None:
        self.assertEqual(
            [path for path in self.parent.iterdir() if ".restore-" in path.name],
            [],
        )


class SuccessTests(RestorationTestCase):
    def test_restores_complete_snapshot_in_inventory_order(self) -> None:
        result = restore_verified_pipeline_state(
            self.state,
            destination=self.destination,
        )

        self.assertIs(type(result), CompletedPipelineStateRestore)
        self.assertEqual(result.snapshot_root, self.destination)
        self.assertEqual(result.run_id, self.run_id)
        self.assertEqual(
            result.inventory_id,
            self.state.acquired_blobs.control.inventory.inventory_id,
        )
        for root_name in ROOT_NAMES:
            root = getattr(result.roots, root_name)
            self.assertEqual(root, self.destination / root_name)
            self.assertTrue(root.is_dir())
        for item in self.state.entries:
            entry = item.inventory_entry
            path = getattr(result.roots, entry.root_name).joinpath(
                *entry.relative_path.split("/")
            )
            self.assertEqual(path.read_bytes(), item.content_bytes)
        self.assert_no_staging_tree()

    def test_duplicate_hash_is_written_to_each_inventory_path(self) -> None:
        result = restore_verified_pipeline_state(
            self.state,
            destination=self.destination,
        )
        first, second = self.state.entries[:2]
        self.assertEqual(first.inventory_entry.sha256, second.inventory_entry.sha256)
        first_path = getattr(
            result.roots,
            first.inventory_entry.root_name,
        ) / first.inventory_entry.relative_path
        second_path = getattr(
            result.roots,
            second.inventory_entry.root_name,
        ) / second.inventory_entry.relative_path
        self.assertEqual(first_path.read_bytes(), second_path.read_bytes())

    def test_exact_existing_snapshot_is_idempotent_and_not_rewritten(self) -> None:
        first = restore_verified_pipeline_state(
            self.state,
            destination=self.destination,
        )
        tracked = first.roots.calendar_data / "a.json"
        first_stat = tracked.stat()

        second = restore_verified_pipeline_state(
            self.state,
            destination=self.destination,
        )

        self.assertEqual(first, second)
        second_stat = tracked.stat()
        self.assertEqual(
            (first_stat.st_ino, first_stat.st_mtime_ns),
            (second_stat.st_ino, second_stat.st_mtime_ns),
        )
        self.assert_no_staging_tree()


class PreMutationValidationTests(RestorationTestCase):
    def test_rejects_relative_destination_before_lock_creation(self) -> None:
        with self.assertRaises(PipelineStateRestorationError):
            restore_verified_pipeline_state(self.state, destination=Path(self.run_id))
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_rejects_destination_not_named_by_run_id(self) -> None:
        with self.assertRaisesRegex(
            PipelineStateRestorationError,
            "^pipeline state restoration destination verification failed$",
        ):
            restore_verified_pipeline_state(
                self.state,
                destination=self.parent / ("f" * 64),
            )
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_rejects_missing_parent(self) -> None:
        missing = self.parent / "missing" / self.run_id
        with self.assertRaises(PipelineStateRestorationError):
            restore_verified_pipeline_state(self.state, destination=missing)
        self.assertFalse(missing.parent.exists())

    def test_rejects_mutated_state_before_filesystem_mutation(self) -> None:
        object.__setattr__(self.state, "entries", self.state.entries[:-1])
        with self.assertRaisesRegex(
            PipelineStateRestorationError,
            "^pipeline state restoration input verification failed$",
        ):
            restore_verified_pipeline_state(
                self.state,
                destination=self.destination,
            )
        self.assertEqual(list(self.parent.iterdir()), [])


class FailureAtomicityTests(RestorationTestCase):
    def test_write_failure_leaves_no_published_or_staging_tree(self) -> None:
        real_write = restoration_module._write_verified_file
        calls = 0

        def fail_second_write(path: Path, content_bytes: bytes) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected secret")
            real_write(path, content_bytes)

        with patch.object(
            restoration_module,
            "_write_verified_file",
            side_effect=fail_second_write,
        ):
            with self.assertRaises(PipelineStateRestorationError) as caught:
                restore_verified_pipeline_state(
                    self.state,
                    destination=self.destination,
                )

        self.assertNotIn("secret", str(caught.exception))
        self.assertFalse(self.destination.exists())
        self.assert_no_staging_tree()

    def test_prepublication_tampering_is_detected_and_cleaned(self) -> None:
        real_write = restoration_module._write_verified_file
        calls = 0

        def tamper_after_second_write(path: Path, content_bytes: bytes) -> None:
            nonlocal calls
            calls += 1
            real_write(path, content_bytes)
            if calls == 2:
                path.write_bytes(b"tampered")

        with patch.object(
            restoration_module,
            "_write_verified_file",
            side_effect=tamper_after_second_write,
        ):
            with self.assertRaisesRegex(
                PipelineStateRestorationError,
                "^pipeline state restoration tree verification failed$",
            ):
                restore_verified_pipeline_state(
                    self.state,
                    destination=self.destination,
                )
        self.assertFalse(self.destination.exists())
        self.assert_no_staging_tree()

    def test_rename_failure_leaves_no_published_or_staging_tree(self) -> None:
        with patch.object(
            restoration_module.os,
            "rename",
            side_effect=OSError("injected secret"),
        ):
            with self.assertRaisesRegex(
                PipelineStateRestorationError,
                "^pipeline state restoration publication failed$",
            ):
                restore_verified_pipeline_state(
                    self.state,
                    destination=self.destination,
                )
        self.assertFalse(self.destination.exists())
        self.assert_no_staging_tree()

    def test_concurrent_destination_is_not_removed_or_overwritten(self) -> None:
        marker = b"concurrent-owner"

        def create_destination_then_fail(source: Path, target: Path) -> None:
            target.mkdir()
            (target / "marker.bin").write_bytes(marker)
            raise FileExistsError()

        with patch.object(
            restoration_module.os,
            "rename",
            side_effect=create_destination_then_fail,
        ):
            with self.assertRaises(PipelineStateRestorationError):
                restore_verified_pipeline_state(
                    self.state,
                    destination=self.destination,
                )
        self.assertEqual((self.destination / "marker.bin").read_bytes(), marker)
        self.assert_no_staging_tree()

    def test_base_exception_is_not_swallowed_and_staging_is_cleaned(self) -> None:
        with patch.object(
            restoration_module,
            "_write_verified_file",
            side_effect=KeyboardInterrupt,
        ):
            with self.assertRaises(KeyboardInterrupt):
                restore_verified_pipeline_state(
                    self.state,
                    destination=self.destination,
                )
        self.assertFalse(self.destination.exists())
        self.assert_no_staging_tree()


class ExistingDestinationTests(RestorationTestCase):
    def test_inconsistent_existing_snapshot_is_rejected_without_overwrite(self) -> None:
        result = restore_verified_pipeline_state(
            self.state,
            destination=self.destination,
        )
        target = result.roots.calendar_data / "a.json"
        target.write_bytes(b"tampered")

        with self.assertRaisesRegex(
            PipelineStateRestorationError,
            "^pipeline state restoration tree verification failed$",
        ):
            restore_verified_pipeline_state(
                self.state,
                destination=self.destination,
            )
        self.assertEqual(target.read_bytes(), b"tampered")
        self.assert_no_staging_tree()

    def test_existing_regular_file_is_rejected_without_removal(self) -> None:
        marker = b"existing-owner"
        self.destination.write_bytes(marker)
        with self.assertRaises(PipelineStateRestorationError):
            restore_verified_pipeline_state(
                self.state,
                destination=self.destination,
            )
        self.assertEqual(self.destination.read_bytes(), marker)

    def test_extra_file_is_rejected(self) -> None:
        restore_verified_pipeline_state(self.state, destination=self.destination)
        extra = self.destination / "calendar_data" / "extra.bin"
        extra.write_bytes(b"extra")
        with self.assertRaises(PipelineStateRestorationError):
            restore_verified_pipeline_state(
                self.state,
                destination=self.destination,
            )
        self.assertEqual(extra.read_bytes(), b"extra")

    def test_hard_linked_snapshot_files_are_rejected(self) -> None:
        result = restore_verified_pipeline_state(self.state, destination=self.destination)
        source = result.roots.calendar_data / "a.json"
        duplicate = result.roots.calendar_data / "duplicate.json"
        duplicate.unlink()
        try:
            os.link(source, duplicate)
        except OSError:
            self.skipTest("hard links are unavailable on this filesystem")
        with self.assertRaises(PipelineStateRestorationError):
            restore_verified_pipeline_state(
                self.state,
                destination=self.destination,
            )
        self.assertEqual(source.read_bytes(), duplicate.read_bytes())


class ResultStrictnessTests(RestorationTestCase):
    def test_result_rejects_mismatched_roots(self) -> None:
        result = restore_verified_pipeline_state(self.state, destination=self.destination)
        object.__setattr__(result.roots, "calendar_data", self.parent)
        with self.assertRaises(PipelineStateRestorationError):
            CompletedPipelineStateRestore(
                snapshot_root=result.snapshot_root,
                roots=result.roots,
                run_id=result.run_id,
                inventory_id=result.inventory_id,
            )

    def test_result_rejects_noncanonical_hashes(self) -> None:
        result = restore_verified_pipeline_state(self.state, destination=self.destination)
        with self.assertRaises(PipelineStateRestorationError):
            CompletedPipelineStateRestore(
                snapshot_root=result.snapshot_root,
                roots=result.roots,
                run_id="G" * 64,
                inventory_id=result.inventory_id,
            )


class CapabilityLockTests(unittest.TestCase):
    def _module_ast(self) -> ast.Module:
        return ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))

    def test_no_network_broker_or_latest_selection_capability(self) -> None:
        forbidden = (
            "google",
            "storage",
            "list_blobs",
            "latest",
            "requests",
            "http",
            "socket",
            "broker",
            "order",
            "notification",
            "subprocess",
            "environ",
            "getenv",
        )
        identifiers: list[str] = []
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Name):
                identifiers.append(node.id.lower())
            elif isinstance(node, ast.Attribute):
                identifiers.append(node.attr.lower())
        offenders = [
            identifier
            for identifier in identifiers
            if any(token in identifier for token in forbidden)
        ]
        self.assertEqual(offenders, [])

    def test_publication_uses_rename_and_never_replace_or_move(self) -> None:
        calls = {
            node.func.attr
            for node in ast.walk(self._module_ast())
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertIn("rename", calls)
        self.assertNotIn("replace", calls)
        self.assertNotIn("move", calls)

    def test_recursive_removal_is_confined_to_private_cleanup_helper(self) -> None:
        offenders: list[str] = []
        module = self._module_ast()
        functions = [
            candidate
            for candidate in ast.walk(module)
            if isinstance(candidate, ast.FunctionDef)
        ]
        for node in ast.walk(module):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr == "rmtree":
                parent = next(
                    (
                        candidate
                        for candidate in functions
                        if node in tuple(ast.walk(candidate))
                    ),
                    None,
                )
                if parent is None or parent.name != "_safe_cleanup":
                    offenders.append(node.func.attr)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
