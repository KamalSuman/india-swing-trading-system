from __future__ import annotations

import ast
import hashlib
import os
import stat
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline import state_inventory
from india_swing.daily_pipeline.models import DailyPipelineRun
from india_swing.daily_pipeline.state_inventory import (
    ROOT_NAMES,
    PipelineStateEntry,
    PipelineStateInventory,
    PipelineStateInventoryError,
    PipelineStateRoots,
    build_pipeline_state_inventory,
    encode_pipeline_state_inventory,
    parse_pipeline_state_inventory,
)

from tests.test_promotion import daily_run as _promotion_daily_run

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MODULE_PATH = _REPO_ROOT / "src" / "india_swing" / "daily_pipeline" / "state_inventory.py"


def _make_roots(base: Path) -> PipelineStateRoots:
    kwargs = {}
    for name in ROOT_NAMES:
        root_path = base / name
        root_path.mkdir(parents=True, exist_ok=True)
        kwargs[name] = root_path
    return PipelineStateRoots(**kwargs)


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


class _FakeStat:
    """A minimal lstat-result stand-in exposing only the attributes this
    module's own code reads, so a directory's identity can be deliberately
    mutated between two lstat calls in a test without touching a real
    filesystem."""

    def __init__(self, base: os.stat_result, **overrides: object) -> None:
        self.st_mode = overrides.get("st_mode", base.st_mode)
        self.st_dev = overrides.get("st_dev", base.st_dev)
        self.st_ino = overrides.get("st_ino", base.st_ino)
        self.st_size = overrides.get("st_size", base.st_size)
        self.st_mtime_ns = overrides.get("st_mtime_ns", base.st_mtime_ns)
        self.st_file_attributes = getattr(base, "st_file_attributes", 0)


class _IsoformatOnly:
    """Exposes isoformat() like a real date/datetime but is not an exact
    instance of either, so it fails PipelineStateInventory's exact-type
    checks while still letting the private inventory_id helper (which only
    calls .isoformat()) succeed without raising."""

    def __init__(self, value: str) -> None:
        self._value = value

    def isoformat(self) -> str:
        return self._value


class _RootsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.roots = _make_roots(self.base)
        self.run = _promotion_daily_run()

    def tearDown(self) -> None:
        self.temporary.cleanup()


class BuildDeterminismTests(_RootsTestCase):
    def test_deterministic_bytes_and_id_across_creation_order(self) -> None:
        with tempfile.TemporaryDirectory() as other_base_name:
            other_base = Path(other_base_name)
            other_roots = _make_roots(other_base)

            _write(self.roots.calendar_data / "a.json", b"alpha")
            _write(self.roots.calendar_data / "sub" / "b.json", b"beta")
            _write(self.roots.daily_pipeline / "runs" / "c.json", b"gamma")

            _write(other_roots.daily_pipeline / "runs" / "c.json", b"gamma")
            _write(other_roots.calendar_data / "sub" / "b.json", b"beta")
            _write(other_roots.calendar_data / "a.json", b"alpha")

            first = build_pipeline_state_inventory(self.run, self.roots)
            second = build_pipeline_state_inventory(self.run, other_roots)

            self.assertEqual(first.inventory_id, second.inventory_id)
            self.assertEqual(
                encode_pipeline_state_inventory(first),
                encode_pipeline_state_inventory(second),
            )

    def test_entries_ordered_by_fixed_root_then_relative_path(self) -> None:
        _write(self.roots.daily_pipeline / "z.json", b"z")
        _write(self.roots.calendar_data / "a.json", b"a")
        _write(self.roots.identity_registry / "m.json", b"m")

        inventory = build_pipeline_state_inventory(self.run, self.roots)
        observed = [(entry.root_name, entry.relative_path) for entry in inventory.entries]
        self.assertEqual(
            observed,
            [
                ("calendar_data", "a.json"),
                ("identity_registry", "m.json"),
                ("daily_pipeline", "z.json"),
            ],
        )

    def test_relative_paths_use_forward_slash(self) -> None:
        _write(self.roots.calendar_data / "sub" / "nested" / "file.json", b"content")
        inventory = build_pipeline_state_inventory(self.run, self.roots)
        self.assertEqual(inventory.entries[0].relative_path, "sub/nested/file.json")

    def test_correct_hash_count_and_total_bytes(self) -> None:
        content_one = b"first-file-content"
        content_two = b"second-file-content-longer"
        _write(self.roots.calendar_data / "one.json", content_one)
        _write(self.roots.daily_reports / "two.json", content_two)

        inventory = build_pipeline_state_inventory(self.run, self.roots)
        self.assertEqual(inventory.entry_count, 2)
        self.assertEqual(
            inventory.total_bytes, len(content_one) + len(content_two)
        )
        by_root = {entry.root_name: entry for entry in inventory.entries}
        self.assertEqual(
            by_root["calendar_data"].sha256, hashlib.sha256(content_one).hexdigest()
        )
        self.assertEqual(by_root["calendar_data"].byte_count, len(content_one))
        self.assertEqual(
            by_root["daily_reports"].sha256, hashlib.sha256(content_two).hexdigest()
        )

    def test_binds_run_session_cutoff_previous_run_id(self) -> None:
        _write(self.roots.calendar_data / "one.json", b"content")
        inventory = build_pipeline_state_inventory(self.run, self.roots)
        self.assertEqual(inventory.run_id, self.run.run_id)
        self.assertEqual(inventory.previous_run_id, self.run.previous_run_id)
        self.assertEqual(inventory.market_session, self.run.market_session)
        self.assertEqual(inventory.cutoff, self.run.cutoff)

    def test_encoded_json_excludes_absolute_temp_root_and_file_content(self) -> None:
        secret_content = b"SECRET-SNOWFLAKE-CONTENT-DO-NOT-LEAK-771a"
        _write(self.roots.calendar_data / "one.json", secret_content)
        inventory = build_pipeline_state_inventory(self.run, self.roots)
        encoded = encode_pipeline_state_inventory(inventory)
        self.assertNotIn(str(self.base).encode("utf-8"), encoded)
        self.assertNotIn(secret_content, encoded)


class LockArtifactExclusionTests(_RootsTestCase):
    def test_locks_directory_and_named_lock_files_are_excluded(self) -> None:
        _write(self.roots.calendar_data / ".locks" / "artifact.lock", b"lock-body")
        _write(self.roots.daily_pipeline / ".daily-runs.lock", b"lock-body")
        _write(self.roots.identity_registry / ".identity-registry.lock", b"lock-body")
        _write(
            self.roots.identity_registry / ".adjudication-queues.lock", b"lock-body"
        )
        _write(self.roots.daily_reports / ".derived-evidence.lock", b"lock-body")
        _write(self.roots.calendar_data / "real.json", b"real-content")

        inventory = build_pipeline_state_inventory(self.run, self.roots)
        self.assertEqual(inventory.entry_count, 1)
        self.assertEqual(inventory.entries[0].relative_path, "real.json")

    def test_similarly_named_artifacts_are_not_silently_ignored(self) -> None:
        _write(self.roots.calendar_data / "business.lock", b"included")
        _write(self.roots.calendar_data / "locks" / "inside.json", b"included-too")

        inventory = build_pipeline_state_inventory(self.run, self.roots)
        relative_paths = {entry.relative_path for entry in inventory.entries}
        self.assertIn("business.lock", relative_paths)
        self.assertIn("locks/inside.json", relative_paths)

    def test_hidden_dotfile_causes_explicit_rejection_not_silent_skip(self) -> None:
        _write(self.roots.calendar_data / ".hidden.json", b"content")
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, self.roots)


class RootValidationTests(_RootsTestCase):
    def test_relative_root_rejected(self) -> None:
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = Path("relative/path")
        with self.assertRaises(PipelineStateInventoryError):
            PipelineStateRoots(**kwargs)

    def test_equal_roots_rejected(self) -> None:
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = kwargs["calendar_data"]
        with self.assertRaises(PipelineStateInventoryError):
            PipelineStateRoots(**kwargs)

    def test_nested_root_rejected(self) -> None:
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = kwargs["calendar_data"] / "nested"
        with self.assertRaises(PipelineStateInventoryError):
            PipelineStateRoots(**kwargs)

    def test_missing_root_rejected_at_build_time(self) -> None:
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = self.base / "does-not-exist"
        roots = PipelineStateRoots(**kwargs)
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, roots)

    def test_root_that_is_a_file_rejected_at_build_time(self) -> None:
        file_path = self.base / "a-file-not-a-directory"
        file_path.write_bytes(b"not a directory")
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = file_path
        roots = PipelineStateRoots(**kwargs)
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, roots)

    def test_root_dot_dot_component_rejected(self) -> None:
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = self.roots.calendar_data / ".." / "escape"
        with self.assertRaises(PipelineStateInventoryError):
            PipelineStateRoots(**kwargs)

    def test_case_only_equal_roots_rejected_on_windows(self) -> None:
        if os.name != "nt":
            self.skipTest("case-insensitive alias check is Windows-specific")
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = Path(str(kwargs["calendar_data"]).upper())
        with self.assertRaises(PipelineStateInventoryError):
            PipelineStateRoots(**kwargs)

    def test_case_only_nested_roots_rejected_on_windows(self) -> None:
        if os.name != "nt":
            self.skipTest("case-insensitive alias check is Windows-specific")
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = Path(str(kwargs["calendar_data"]).upper()) / "NESTED"
        with self.assertRaises(PipelineStateInventoryError):
            PipelineStateRoots(**kwargs)

    def test_root_symlink_rejected_at_build_time(self) -> None:
        real_dir = self.base / "real-target-dir"
        real_dir.mkdir()
        link_path = self.base / "root-symlink"
        try:
            os.symlink(real_dir, link_path, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("platform cannot create symlinks")
        kwargs = {name: getattr(self.roots, name) for name in ROOT_NAMES}
        kwargs["daily_pipeline"] = link_path
        roots = PipelineStateRoots(**kwargs)
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, roots)


class WalkSafetyTests(_RootsTestCase):
    def test_nested_file_symlink_rejected(self) -> None:
        target = self.base / "target.json"
        target.write_bytes(b"target-content")
        link_path = self.roots.calendar_data / "link.json"
        try:
            os.symlink(target, link_path)
        except (OSError, NotImplementedError):
            self.skipTest("platform cannot create symlinks")
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, self.roots)

    def test_nested_directory_symlink_rejected(self) -> None:
        target_dir = self.base / "target-dir"
        target_dir.mkdir()
        _write(target_dir / "inner.json", b"inner")
        link_path = self.roots.calendar_data / "link-dir"
        try:
            os.symlink(target_dir, link_path, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("platform cannot create symlinks")
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, self.roots)

    def test_fifo_rejected(self) -> None:
        if not hasattr(os, "mkfifo"):
            self.skipTest("platform cannot create FIFOs")
        fifo_path = self.roots.calendar_data / "pipe"
        try:
            os.mkfifo(fifo_path)
        except OSError:
            self.skipTest("platform refused FIFO creation")
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, self.roots)

    def test_empty_file_rejected(self) -> None:
        _write(self.roots.calendar_data / "empty.json", b"")
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, self.roots)

    def test_oversized_file_rejected(self) -> None:
        _write(self.roots.calendar_data / "big.json", b"0123456789")
        with patch.object(state_inventory, "MAXIMUM_FILE_BYTES", 5):
            with self.assertRaises(PipelineStateInventoryError):
                build_pipeline_state_inventory(self.run, self.roots)

    def test_unreadable_directory_rejected(self) -> None:
        _write(self.roots.calendar_data / "some.json", b"content")
        with patch.object(
            state_inventory.os, "scandir", side_effect=OSError("boom")
        ):
            with self.assertRaises(PipelineStateInventoryError):
                build_pipeline_state_inventory(self.run, self.roots)

    def test_concurrent_mutation_rejected(self) -> None:
        _write(self.roots.calendar_data / "some.json", b"content")
        with patch.object(
            state_inventory,
            "read_stable_regular_file",
            side_effect=state_inventory.FileSafetyError("changed"),
        ):
            with self.assertRaises(PipelineStateInventoryError):
                build_pipeline_state_inventory(self.run, self.roots)

    def test_entry_count_ceiling_rejected(self) -> None:
        _write(self.roots.calendar_data / "one.json", b"a")
        _write(self.roots.calendar_data / "two.json", b"b")
        with patch.object(state_inventory, "MAXIMUM_INCLUDED_FILES", 1):
            with self.assertRaises(PipelineStateInventoryError):
                build_pipeline_state_inventory(self.run, self.roots)

    def test_total_bytes_ceiling_rejected(self) -> None:
        _write(self.roots.calendar_data / "one.json", b"aaaaaaaaaa")
        _write(self.roots.daily_reports / "two.json", b"bbbbbbbbbb")
        with patch.object(state_inventory, "MAXIMUM_TOTAL_BYTES", 15):
            with self.assertRaises(PipelineStateInventoryError):
                build_pipeline_state_inventory(self.run, self.roots)

    def test_encoded_bytes_ceiling_rejected(self) -> None:
        _write(self.roots.calendar_data / "one.json", b"a" * 200)
        inventory = build_pipeline_state_inventory(self.run, self.roots)
        with patch.object(state_inventory, "MAXIMUM_ENCODED_BYTES", 10):
            with self.assertRaises(PipelineStateInventoryError):
                encode_pipeline_state_inventory(inventory)

    def test_unsafe_path_segment_rejected(self) -> None:
        _write(self.roots.calendar_data / "bad name.json", b"content")
        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, self.roots)

    def test_casefold_collision_rejected_via_direct_construction(self) -> None:
        first = PipelineStateEntry(
            root_name="calendar_data", relative_path="Foo.json", byte_count=1, sha256="0" * 64
        )
        second = PipelineStateEntry(
            root_name="calendar_data", relative_path="foo.json", byte_count=1, sha256="1" * 64
        )
        with self.assertRaises(PipelineStateInventoryError):
            PipelineStateInventory(
                schema_version=1,
                run_id=self.run.run_id,
                previous_run_id=self.run.previous_run_id,
                market_session=self.run.market_session,
                cutoff=self.run.cutoff,
                entries=(first, second),
                entry_count=2,
                total_bytes=2,
            )

    def test_secret_bearing_failure_has_no_leakage_or_chaining(self) -> None:
        secret_name = "SECRET-DIRECTORY-DO-NOT-LEAK-8b2c"
        _write(self.roots.calendar_data / f"{secret_name}.json", b"x" * 20)
        with patch.object(state_inventory, "MAXIMUM_FILE_BYTES", 1):
            try:
                build_pipeline_state_inventory(self.run, self.roots)
                self.fail("expected PipelineStateInventoryError")
            except PipelineStateInventoryError as exc:
                self.assertNotIn(secret_name, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)


class DataclassStrictnessTests(_RootsTestCase):
    def _valid_entry(self, **overrides: object) -> PipelineStateEntry:
        kwargs: dict[str, object] = dict(
            root_name="calendar_data",
            relative_path="file.json",
            byte_count=10,
            sha256="a" * 64,
        )
        kwargs.update(overrides)
        return PipelineStateEntry(**kwargs)

    def _valid_inventory(self, **overrides: object) -> PipelineStateInventory:
        entry = self._valid_entry()
        kwargs: dict[str, object] = dict(
            schema_version=1,
            run_id=self.run.run_id,
            previous_run_id=self.run.previous_run_id,
            market_session=self.run.market_session,
            cutoff=self.run.cutoff,
            entries=(entry,),
            entry_count=1,
            total_bytes=10,
        )
        kwargs.update(overrides)
        return PipelineStateInventory(**kwargs)

    def test_roots_type_rejected_at_build(self) -> None:
        class _FakeRoots:
            calendar_data = Path("/a")
            identity_registry = Path("/b")
            historical_prices = Path("/c")
            daily_reports = Path("/d")
            reference_data = Path("/e")
            daily_pipeline = Path("/f")

        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(self.run, _FakeRoots())

    def test_run_type_rejected_at_build(self) -> None:
        class _FakeRun:
            run_id = "a" * 64

        with self.assertRaises(PipelineStateInventoryError):
            build_pipeline_state_inventory(_FakeRun(), self.roots)

    def test_entry_subclass_rejected(self) -> None:
        class _ShapedEntry(PipelineStateEntry):
            pass

        shaped = _ShapedEntry(
            root_name="calendar_data", relative_path="file.json", byte_count=1, sha256="a" * 64
        )
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(entries=(shaped,), entry_count=1, total_bytes=1)

    def test_post_construction_mutation_of_entries_detected(self) -> None:
        inventory = self._valid_inventory()
        tampered_entry = self._valid_entry(sha256="b" * 64)
        object.__setattr__(inventory, "entries", (tampered_entry,))
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()

    def test_post_construction_mutation_of_total_bytes_detected(self) -> None:
        inventory = self._valid_inventory()
        object.__setattr__(inventory, "total_bytes", inventory.total_bytes + 1)
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()

    def test_byte_count_bool_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_entry(byte_count=True)

    def test_hash_uppercase_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_entry(sha256="A" * 64)

    def test_hash_short_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_entry(sha256="a" * 63)

    def test_invalid_root_name_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_entry(root_name="not_a_real_root")

    def test_entries_wrong_order_rejected(self) -> None:
        first = self._valid_entry(relative_path="b.json")
        second = self._valid_entry(relative_path="a.json")
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(entries=(first, second), entry_count=2, total_bytes=20)

    def test_duplicate_entries_rejected(self) -> None:
        entry = self._valid_entry()
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(entries=(entry, entry), entry_count=2, total_bytes=20)

    def test_count_mismatch_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(entry_count=2)

    def test_total_bytes_mismatch_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(total_bytes=999)

    def test_unsupported_schema_version_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(schema_version=2)

    def test_schema_version_bool_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(schema_version=True)

    def test_run_id_wrong_length_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(run_id="a" * 63)

    def test_entries_not_tuple_rejected(self) -> None:
        entry = self._valid_entry()
        with self.assertRaises(PipelineStateInventoryError):
            self._valid_inventory(entries=[entry], entry_count=1, total_bytes=10)

    def test_constructor_rejects_entry_count_ceiling_plus_one(self) -> None:
        entries = tuple(
            self._valid_entry(relative_path=f"file{i}.json") for i in range(3)
        )
        with patch.object(state_inventory, "MAXIMUM_INCLUDED_FILES", 2):
            with self.assertRaises(PipelineStateInventoryError):
                self._valid_inventory(entries=entries, entry_count=3, total_bytes=30)

    def test_constructor_rejects_total_bytes_ceiling_plus_one(self) -> None:
        entries = tuple(
            self._valid_entry(relative_path=f"file{i}.json", byte_count=10)
            for i in range(3)
        )
        with patch.object(state_inventory, "MAXIMUM_TOTAL_BYTES", 29):
            with self.assertRaises(PipelineStateInventoryError):
                self._valid_inventory(entries=entries, entry_count=3, total_bytes=30)

    def _mutated_and_recomputed(self, **overrides: object) -> PipelineStateInventory:
        """Build a valid inventory, mutate a field via object.__setattr__, then
        recompute and install inventory_id through the same private helper an
        attacker would use -- proving verify_content_identity still rejects
        the mutation on its own scalar/aggregate checks regardless of the
        hash matching."""

        inventory = self._valid_inventory()
        for name, value in overrides.items():
            object.__setattr__(inventory, name, value)
        object.__setattr__(
            inventory, "inventory_id", inventory._calculated_inventory_id()
        )
        return inventory

    def test_verify_rejects_mutated_schema_version_despite_recomputed_inventory_id(
        self,
    ) -> None:
        inventory = self._mutated_and_recomputed(schema_version=2)
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()

    def test_verify_rejects_mutated_run_id_despite_recomputed_inventory_id(self) -> None:
        inventory = self._mutated_and_recomputed(run_id="z" * 64)
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()

    def test_verify_rejects_mutated_previous_run_id_despite_recomputed_inventory_id(
        self,
    ) -> None:
        inventory = self._mutated_and_recomputed(previous_run_id="not-a-hash")
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()

    def test_verify_rejects_mutated_market_session_despite_recomputed_inventory_id(
        self,
    ) -> None:
        # An object exposing only isoformat() (not an exact date) lets the
        # private inventory_id helper succeed -- proving verify's own exact
        # type check, not the hash, is what catches this mutation.
        inventory = self._mutated_and_recomputed(
            market_session=_IsoformatOnly(self.run.market_session.isoformat())
        )
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()

    def test_verify_rejects_mutated_cutoff_despite_recomputed_inventory_id(self) -> None:
        inventory = self._mutated_and_recomputed(
            cutoff=_IsoformatOnly(self.run.cutoff.isoformat())
        )
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()

    def test_verify_rejects_mutated_entry_count_bool_despite_recomputed_inventory_id(
        self,
    ) -> None:
        inventory = self._mutated_and_recomputed(entry_count=True)
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()

    def test_verify_rejects_mutated_total_bytes_bool_despite_recomputed_inventory_id(
        self,
    ) -> None:
        inventory = self._mutated_and_recomputed(total_bytes=True)
        with self.assertRaises(PipelineStateInventoryError):
            inventory.verify_content_identity()


class CodecStrictnessTests(_RootsTestCase):
    def _built(self) -> PipelineStateInventory:
        _write(self.roots.calendar_data / "one.json", b"content-one")
        return build_pipeline_state_inventory(self.run, self.roots)

    def test_round_trip_encode_parse(self) -> None:
        inventory = self._built()
        encoded = encode_pipeline_state_inventory(inventory)
        parsed = parse_pipeline_state_inventory(encoded)
        self.assertEqual(parsed.inventory_id, inventory.inventory_id)
        self.assertEqual(encode_pipeline_state_inventory(parsed), encoded)

    def test_unknown_top_level_key_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        tampered = encoded[:-2] + b',"extra_unknown_key":1}\n'
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(tampered)

    def test_missing_top_level_key_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        tampered = encoded.replace(b'"total_bytes":', b'"removed_bytes":')
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(tampered)

    def test_duplicate_json_key_rejected(self) -> None:
        payload = (
            b'{"cutoff":"2026-01-01T00:00:00+00:00","cutoff":"2026-01-01T00:00:00+00:00",'
            b'"entries":[],"entry_count":0,"inventory_id":"' + b"a" * 64 + b'",'
            b'"market_session":"2026-01-01","previous_run_id":null,'
            b'"run_id":"' + b"a" * 64 + b'","schema_version":1,"total_bytes":0}\n'
        )
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(payload)

    def test_float_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        tampered = encoded.replace(b'"total_bytes":11', b'"total_bytes":11.0')
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(tampered)

    def test_nan_constant_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        tampered = encoded.replace(b'"total_bytes":11', b'"total_bytes":NaN')
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(tampered)

    def test_bool_as_int_schema_version_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        tampered = encoded.replace(b'"schema_version":1', b'"schema_version":true')
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(tampered)

    def test_malformed_utf8_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(b"\xff\xfe\x00\x01")

    def test_malformed_json_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(b"{not valid json")

    def test_noncanonical_whitespace_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        tampered = encoded.replace(b'"schema_version":1', b'"schema_version": 1')
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(tampered)

    def test_noncanonical_key_order_rejected(self) -> None:
        payload = (
            b'{"run_id":"' + b"a" * 64 + b'","schema_version":1,'
            b'"previous_run_id":null,"market_session":"2026-01-01",'
            b'"cutoff":"2026-01-01T00:00:00+00:00","entries":[],'
            b'"entry_count":0,"total_bytes":0,"inventory_id":"' + b"b" * 64 + b'"}\n'
        )
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(payload)

    def test_noncanonical_timestamp_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        cutoff_field = b'"cutoff":"' + self._built().cutoff.isoformat().encode("utf-8") + b'"'
        self.assertIn(cutoff_field, encoded)
        z_form = cutoff_field.replace(b"+00:00", b"Z")
        tampered = encoded.replace(cutoff_field, z_form)
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(tampered)

    def test_missing_final_newline_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        self.assertTrue(encoded.endswith(b"\n"))
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(encoded[:-1])

    def test_extra_trailing_newline_rejected(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(encoded + b"\n")

    def test_inventory_id_mismatch_rejected(self) -> None:
        inventory = self._built()
        encoded = encode_pipeline_state_inventory(inventory)
        tampered_id = ("f" if inventory.inventory_id[0] != "f" else "e") + inventory.inventory_id[1:]
        tampered = encoded.replace(
            f'"inventory_id":"{inventory.inventory_id}"'.encode("utf-8"),
            f'"inventory_id":"{tampered_id}"'.encode("utf-8"),
        )
        self.assertNotEqual(tampered, encoded)
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(tampered)

    def test_payload_not_bytes_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory("not-bytes")  # type: ignore[arg-type]

    def test_empty_payload_rejected(self) -> None:
        with self.assertRaises(PipelineStateInventoryError):
            parse_pipeline_state_inventory(b"")

    def test_oversized_payload_rejected(self) -> None:
        with patch.object(state_inventory, "MAXIMUM_ENCODED_BYTES", 4):
            with self.assertRaises(PipelineStateInventoryError):
                parse_pipeline_state_inventory(b"12345")

    def test_parser_rejects_entry_count_ceiling_plus_one(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        with patch.object(state_inventory, "MAXIMUM_INCLUDED_FILES", 0):
            with self.assertRaises(PipelineStateInventoryError):
                parse_pipeline_state_inventory(encoded)

    def test_parser_rejects_total_bytes_ceiling_plus_one(self) -> None:
        encoded = encode_pipeline_state_inventory(self._built())
        with patch.object(state_inventory, "MAXIMUM_TOTAL_BYTES", 0):
            with self.assertRaises(PipelineStateInventoryError):
                parse_pipeline_state_inventory(encoded)


class HardeningRegressionTests(_RootsTestCase):
    def test_build_sanitizes_mutated_run_verification_failure(self) -> None:
        _write(self.roots.calendar_data / "one.json", b"content")
        secret = "SECRET-NESTED-RUN-VERIFICATION-FAILURE-DO-NOT-LEAK-5f1a"
        with patch.object(
            DailyPipelineRun,
            "verify_content_identity",
            side_effect=RuntimeError(secret),
        ):
            try:
                build_pipeline_state_inventory(self.run, self.roots)
                self.fail("expected PipelineStateInventoryError")
            except PipelineStateInventoryError as exc:
                self.assertNotIn(secret, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_encode_sanitizes_hostile_post_construction_scalar_mutation(self) -> None:
        _write(self.roots.calendar_data / "one.json", b"content")
        inventory = build_pipeline_state_inventory(self.run, self.roots)

        class _Hostile:
            pass

        object.__setattr__(inventory, "cutoff", _Hostile())
        with patch.object(PipelineStateInventory, "verify_content_identity", lambda self: None):
            with self.assertRaises(PipelineStateInventoryError):
                encode_pipeline_state_inventory(inventory)

    def test_directory_identity_change_between_collection_and_final_verification_rejected(
        self,
    ) -> None:
        _write(self.roots.calendar_data / "sub" / "file.json", b"content")
        target_dir = self.roots.calendar_data / "sub"
        real_lstat = os.lstat
        call_count = {"n": 0}

        def _fake_lstat(path, *args, **kwargs):
            result = real_lstat(path, *args, **kwargs)
            if Path(path) == target_dir:
                call_count["n"] += 1
                if call_count["n"] > 2:
                    return _FakeStat(result, st_ino=result.st_ino + 1)
            return result

        with patch.object(state_inventory.os, "lstat", side_effect=_fake_lstat):
            try:
                build_pipeline_state_inventory(self.run, self.roots)
                self.fail("expected PipelineStateInventoryError")
            except PipelineStateInventoryError as exc:
                self.assertNotIn(str(target_dir), str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_directory_child_name_change_between_collection_and_final_verification_rejected(
        self,
    ) -> None:
        _write(self.roots.calendar_data / "sub" / "file.json", b"content")
        target_dir = self.roots.calendar_data / "sub"
        real_scandir_names = state_inventory._scandir_names
        call_count = {"n": 0}

        def _fake_scandir_names(path):
            names = real_scandir_names(path)
            if Path(path) == target_dir:
                call_count["n"] += 1
                if call_count["n"] > 1:
                    return names + ["extra-file.json"]
            return names

        with patch.object(state_inventory, "_scandir_names", side_effect=_fake_scandir_names):
            try:
                build_pipeline_state_inventory(self.run, self.roots)
                self.fail("expected PipelineStateInventoryError")
            except PipelineStateInventoryError as exc:
                self.assertNotIn(str(target_dir), str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_final_directory_scan_is_bracketed_by_post_scan_lstat(self) -> None:
        _write(self.roots.calendar_data / "sub" / "file.json", b"content")
        target_dir = self.roots.calendar_data / "sub"
        real_lstat = os.lstat
        call_count = {"n": 0}

        def _fake_lstat(path, *args, **kwargs):
            result = real_lstat(path, *args, **kwargs)
            if Path(path) == target_dir:
                call_count["n"] += 1
                if call_count["n"] == 4:
                    return _FakeStat(result, st_ino=result.st_ino + 1)
            return result

        with patch.object(state_inventory.os, "lstat", side_effect=_fake_lstat):
            with self.assertRaises(PipelineStateInventoryError):
                build_pipeline_state_inventory(self.run, self.roots)

    def test_deleted_nested_entry_field_is_sanitized(self) -> None:
        _write(self.roots.calendar_data / "one.json", b"content")
        inventory = build_pipeline_state_inventory(self.run, self.roots)
        object.__delattr__(inventory.entries[0], "sha256")
        try:
            inventory.verify_content_identity()
            self.fail("expected PipelineStateInventoryError")
        except PipelineStateInventoryError as exc:
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)

    def test_run_mutation_during_collection_is_rejected(self) -> None:
        _write(self.roots.calendar_data / "one.json", b"content")
        real_verify_directory = state_inventory._verify_directory_unchanged
        mutated = {"done": False}

        def _mutating_verify(path, identity, names):
            if not mutated["done"]:
                object.__setattr__(self.run, "previous_run_id", "0" * 64)
                mutated["done"] = True
            return real_verify_directory(path, identity, names)

        with patch.object(
            state_inventory,
            "_verify_directory_unchanged",
            side_effect=_mutating_verify,
        ):
            with self.assertRaises(PipelineStateInventoryError):
                build_pipeline_state_inventory(self.run, self.roots)


class CapabilityLockTests(unittest.TestCase):
    _ALLOWED_IMPORTS = frozenset(
        {
            ("__future__",),
            ("hashlib",),
            ("json",),
            ("os",),
            ("stat",),
            ("dataclasses",),
            ("datetime",),
            ("pathlib",),
            ("india_swing._filesystem",),
            ("india_swing.daily_pipeline.models",),
        }
    )
    _FORBIDDEN_TOKENS = (
        "socket",
        "subprocess",
        "tempfile",
        "shutil",
        "environ",
        "getenv",
        "requests",
        "urllib",
        "http",
        "storage",
        "gcs",
        "gcloud",
        "boto",
        "unlink",
        "remove",
        "rename",
        "rmdir",
        "rmtree",
        "mkstemp",
        "mkdtemp",
        "popen",
        "system",
        "exec",
        "eval",
    )
    _EXACT_FORBIDDEN_NAMES = frozenset({"now", "open"})

    def _module_ast(self) -> ast.Module:
        source = _MODULE_PATH.read_text(encoding="utf-8")
        return ast.parse(source)

    def test_imports_match_an_exact_allowlist(self) -> None:
        tree = self._module_ast()
        observed: set[tuple[str, ...]] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    observed.add(tuple(alias.name.split(".")[:1]))
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level > 0 or module.startswith("india_swing"):
                    observed.add(("india_swing",))
                else:
                    observed.add((module.split(".")[0],))
        allowed_top_level = {item[0] for item in self._ALLOWED_IMPORTS} | {"india_swing"}
        for name_tuple in observed:
            self.assertIn(name_tuple[0], allowed_top_level, name_tuple)

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

    def test_no_import_time_side_effect_calls_at_module_scope(self) -> None:
        tree = self._module_ast()
        for node in tree.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                self.fail("module-level call expression found")


if __name__ == "__main__":
    unittest.main()
