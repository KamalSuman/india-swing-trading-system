from __future__ import annotations

import ast
import inspect
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_input_snapshot as snap_module
from india_swing.promoted_operational_assembly import encode_promoted_operational_assembly_spec
from india_swing.promoted_operational_cloud_control import PromotedOperationalCloudRunControl
from india_swing.promoted_operational_input_snapshot import (
    FILE_INPUT_NAME,
    INPUT_NAMES,
    MAXIMUM_ENCODED_BYTES,
    ROOT_INPUT_NAMES,
    PromotedOperationalInputEntry,
    PromotedOperationalInputInventory,
    PromotedOperationalInputSnapshotError,
    build_promoted_operational_input_inventory,
    decode_promoted_operational_input_inventory,
    encode_promoted_operational_input_inventory,
    input_destination_path,
    verify_hydrated_input_inventory,
)

from tests import test_promoted_operational_job as _job_tests


def _roots(base: Path) -> dict[str, Path]:
    return {name: base / name.replace("_", "-") for name in ROOT_INPUT_NAMES}


class _Fixture:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        tmp_path.mkdir(parents=True, exist_ok=True)
        _preparation, _run_spec, _portfolio, _artifact, self.spec = _job_tests._fixture()
        self.assembly_spec_file = tmp_path / "assembly.json"
        self.assembly_spec_file.write_bytes(encode_promoted_operational_assembly_spec(self.spec))
        self.state_root = tmp_path / "state"
        self.roots = _roots(tmp_path)
        for path in self.roots.values():
            path.mkdir(parents=True, exist_ok=True)

    def control(self, **overrides) -> PromotedOperationalCloudRunControl:
        kwargs: dict[str, object] = dict(
            expected_assembly_spec_id=self.spec.assembly_spec_id,
            expected_operational_run_spec_id="b" * 64,
            target_session=self.spec.target_session,
            state_bucket=self.spec.binding_bucket,
            assembly_spec_file=self.assembly_spec_file,
            state_root=self.state_root,
            **self.roots,
        )
        kwargs.update(overrides)
        return PromotedOperationalCloudRunControl(**kwargs)


class InventoryCodecTests(unittest.TestCase):
    def test_deterministic_inventory_and_snapshot_id_across_rebuilds(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx1 = _Fixture(Path(directory).resolve() / "a")
            (fx1.roots["reference_root"]).mkdir(parents=True, exist_ok=True)
            (fx1.roots["reference_root"] / "x.txt").write_bytes(b"hello")
            inv1 = build_promoted_operational_input_inventory(fx1.control())

            fx2 = _Fixture(Path(directory).resolve() / "b")
            (fx2.roots["reference_root"] / "x.txt").write_bytes(b"hello")
            inv2 = build_promoted_operational_input_inventory(fx2.control())

            self.assertEqual(inv1.inventory_id, inv2.inventory_id)

    def test_canonical_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["promoted_root"] / "y.json").write_bytes(b'{"a":1}')
            inventory = build_promoted_operational_input_inventory(fx.control())
            encoded = encode_promoted_operational_input_inventory(inventory)
            reloaded = decode_promoted_operational_input_inventory(encoded)
            self.assertEqual(reloaded.inventory_id, inventory.inventory_id)
            self.assertEqual(encode_promoted_operational_input_inventory(reloaded), encoded)

    def test_exact_fixed_input_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["portfolio_artifact_root"] / "z.txt").write_bytes(b"z")
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"a")
            inventory = build_promoted_operational_input_inventory(fx.control())
            names = [entry.input_name for entry in inventory.entries]
            self.assertEqual(names, sorted(names, key=INPUT_NAMES.index))
            self.assertEqual(inventory.entries[0].input_name, FILE_INPUT_NAME)

    def test_empty_root_is_valid_and_represented_by_a_successful_scan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            self.assertEqual(inventory.entry_count, 1)
            self.assertEqual(inventory.entries[0].input_name, FILE_INPUT_NAME)

    def test_decode_rejects_duplicate_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            encoded = encode_promoted_operational_input_inventory(inventory)
            text = encoded.decode("utf-8")
            tampered = text[:-2] + f',"total_bytes":{inventory.total_bytes}}}\n'
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(tampered.encode("utf-8"))

    def test_decode_rejects_unknown_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            encoded = encode_promoted_operational_input_inventory(inventory)
            text = encoded.decode("utf-8")[:-2] + ',"extra":"x"}\n'
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(text.encode("utf-8"))

    def test_decode_rejects_missing_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            root = json.loads(encode_promoted_operational_input_inventory(inventory).decode("utf-8"))
            del root["total_bytes"]
            tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(tampered)

    def test_decode_rejects_malformed_entry_input_name(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            inventory = build_promoted_operational_input_inventory(fx.control())
            root = json.loads(encode_promoted_operational_input_inventory(inventory).decode("utf-8"))
            root["entries"][1]["input_name"] = "state_root"
            tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(tampered)

    def test_decode_rejects_bool_byte_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            root = json.loads(encode_promoted_operational_input_inventory(inventory).decode("utf-8"))
            root["entries"][0]["byte_count"] = True
            tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(tampered)

    def test_decode_rejects_malformed_assembly_spec_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            root = json.loads(encode_promoted_operational_input_inventory(inventory).decode("utf-8"))
            root["expected_assembly_spec_id"] = "not-a-sha256"
            tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(tampered)

    def test_decode_rejects_malformed_date(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            root = json.loads(encode_promoted_operational_input_inventory(inventory).decode("utf-8"))
            root["target_session"] = "not-a-date"
            tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(tampered)

    def test_decode_rejects_float_literal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            text = encode_promoted_operational_input_inventory(inventory).decode("utf-8")
            tampered = text.replace('"schema_version":1', '"schema_version":1.0')
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(tampered.encode("utf-8"))

    def test_decode_rejects_nan_literal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            text = encode_promoted_operational_input_inventory(inventory).decode("utf-8")
            tampered = text.replace('"schema_version":1', '"schema_version":NaN')
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(tampered.encode("utf-8"))

    def test_decode_rejects_invalid_utf8(self) -> None:
        with self.assertRaises(PromotedOperationalInputSnapshotError):
            decode_promoted_operational_input_inventory(b"\xff\xfe not utf-8")

    def test_decode_rejects_noncanonical_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            pretty = encode_promoted_operational_input_inventory(inventory).decode("utf-8").replace(",", ", ")
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                decode_promoted_operational_input_inventory(pretty.encode("utf-8"))

    def test_decode_rejects_empty_and_oversized_bytes(self) -> None:
        with self.assertRaises(PromotedOperationalInputSnapshotError):
            decode_promoted_operational_input_inventory(b"")
        with self.assertRaises(PromotedOperationalInputSnapshotError):
            decode_promoted_operational_input_inventory(b"{}" + b" " * MAXIMUM_ENCODED_BYTES)

    def test_manifest_rejects_duplicate_relative_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            inventory = build_promoted_operational_input_inventory(fx.control())
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                PromotedOperationalInputInventory(
                    schema_version=inventory.schema_version,
                    expected_assembly_spec_id=inventory.expected_assembly_spec_id,
                    target_session=inventory.target_session,
                    input_names=inventory.input_names,
                    entries=inventory.entries + (inventory.entries[-1],),
                    entry_count=inventory.entry_count + 1,
                    total_bytes=inventory.total_bytes + inventory.entries[-1].byte_count,
                )

    def test_rejects_wrong_order_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            inventory = build_promoted_operational_input_inventory(fx.control())
            reordered = tuple(reversed(inventory.entries))
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                PromotedOperationalInputInventory(
                    schema_version=inventory.schema_version,
                    expected_assembly_spec_id=inventory.expected_assembly_spec_id,
                    target_session=inventory.target_session,
                    input_names=inventory.input_names,
                    entries=reordered,
                    entry_count=inventory.entry_count,
                    total_bytes=inventory.total_bytes,
                )

    def test_rejects_missing_assembly_spec_file_entry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            inventory = build_promoted_operational_input_inventory(fx.control())
            without_file = tuple(e for e in inventory.entries if e.input_name != FILE_INPUT_NAME)
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                PromotedOperationalInputInventory(
                    schema_version=inventory.schema_version,
                    expected_assembly_spec_id=inventory.expected_assembly_spec_id,
                    target_session=inventory.target_session,
                    input_names=inventory.input_names,
                    entries=without_file,
                    entry_count=len(without_file),
                    total_bytes=sum(e.byte_count for e in without_file),
                )

    def test_input_names_survives_canonical_round_trip_and_empty_roots_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            # No files in any of the eleven roots -- only the mandatory
            # assembly-spec-file entry exists -- yet input_names must still
            # explicitly bind and byte-represent all twelve fixed names.
            inventory = build_promoted_operational_input_inventory(fx.control())
            self.assertEqual(inventory.input_names, INPUT_NAMES)
            encoded = encode_promoted_operational_input_inventory(inventory)
            self.assertIn(b'"input_names"', encoded)
            for root_name in ROOT_INPUT_NAMES:
                self.assertIn(root_name.encode("utf-8"), encoded)
            reloaded = decode_promoted_operational_input_inventory(encoded)
            self.assertEqual(reloaded.input_names, INPUT_NAMES)

    def test_wrong_or_missing_input_names_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())

            with self.subTest(case="reordered"):
                with self.assertRaises(PromotedOperationalInputSnapshotError):
                    PromotedOperationalInputInventory(
                        schema_version=inventory.schema_version,
                        expected_assembly_spec_id=inventory.expected_assembly_spec_id,
                        target_session=inventory.target_session,
                        input_names=tuple(reversed(inventory.input_names)),
                        entries=inventory.entries,
                        entry_count=inventory.entry_count,
                        total_bytes=inventory.total_bytes,
                    )

            with self.subTest(case="missing_one"):
                with self.assertRaises(PromotedOperationalInputSnapshotError):
                    PromotedOperationalInputInventory(
                        schema_version=inventory.schema_version,
                        expected_assembly_spec_id=inventory.expected_assembly_spec_id,
                        target_session=inventory.target_session,
                        input_names=inventory.input_names[:-1],
                        entries=inventory.entries,
                        entry_count=inventory.entry_count,
                        total_bytes=inventory.total_bytes,
                    )

            with self.subTest(case="duplicate"):
                with self.assertRaises(PromotedOperationalInputSnapshotError):
                    PromotedOperationalInputInventory(
                        schema_version=inventory.schema_version,
                        expected_assembly_spec_id=inventory.expected_assembly_spec_id,
                        target_session=inventory.target_session,
                        input_names=inventory.input_names + (inventory.input_names[0],),
                        entries=inventory.entries,
                        entry_count=inventory.entry_count,
                        total_bytes=inventory.total_bytes,
                    )

    def test_str_subclass_input_names_element_is_rejected(self) -> None:
        class _StrSubclass(str):
            pass

        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            subclassed_names = tuple(_StrSubclass(name) for name in inventory.input_names)
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                PromotedOperationalInputInventory(
                    schema_version=inventory.schema_version,
                    expected_assembly_spec_id=inventory.expected_assembly_spec_id,
                    target_session=inventory.target_session,
                    input_names=subclassed_names,
                    entries=inventory.entries,
                    entry_count=inventory.entry_count,
                    total_bytes=inventory.total_bytes,
                )

    def test_input_names_is_replaced_with_the_canonical_detached_tuple(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            # A distinct-but-equal caller-owned tuple (never the module's own
            # INPUT_NAMES object identity).
            caller_owned = tuple(list(inventory.input_names))
            self.assertIsNot(caller_owned, INPUT_NAMES)

            rebuilt = PromotedOperationalInputInventory(
                schema_version=inventory.schema_version,
                expected_assembly_spec_id=inventory.expected_assembly_spec_id,
                target_session=inventory.target_session,
                input_names=caller_owned,
                entries=inventory.entries,
                entry_count=inventory.entry_count,
                total_bytes=inventory.total_bytes,
            )
            # The stored value is the canonical detached tuple, not the
            # caller-owned alias -- later mutation of the caller's own
            # tuple/list cannot reach the retained value.
            self.assertIs(rebuilt.input_names, INPUT_NAMES)

    def test_post_construction_input_names_mutation_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            inventory = build_promoted_operational_input_inventory(fx.control())
            object.__setattr__(inventory, "input_names", tuple(reversed(inventory.input_names)))
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                inventory.verify_content_identity()

    def test_entry_rejects_unknown_input_name(self) -> None:
        with self.assertRaises(PromotedOperationalInputSnapshotError):
            PromotedOperationalInputEntry(
                input_name="state_root", relative_path="x", byte_count=1, sha256="a" * 64,
            )

    def test_entry_rejects_nonempty_relative_path_for_file(self) -> None:
        with self.assertRaises(PromotedOperationalInputSnapshotError):
            PromotedOperationalInputEntry(
                input_name=FILE_INPUT_NAME, relative_path="not-empty", byte_count=1, sha256="a" * 64,
            )

    def test_entry_rejects_traversing_relative_path(self) -> None:
        with self.assertRaises(PromotedOperationalInputSnapshotError):
            PromotedOperationalInputEntry(
                input_name="reference_root", relative_path="../escape", byte_count=1, sha256="a" * 64,
            )

    def test_post_construction_mutation_is_detected_by_verify_content_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            inventory = build_promoted_operational_input_inventory(fx.control())
            object.__setattr__(inventory, "total_bytes", inventory.total_bytes + 1)
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                inventory.verify_content_identity()


class FilesystemInventoryTests(unittest.TestCase):
    def test_missing_root_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            import shutil

            shutil.rmtree(fx.roots["reference_root"])
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                build_promoted_operational_input_inventory(fx.control())

    def test_root_that_is_a_file_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            import shutil

            shutil.rmtree(fx.roots["reference_root"])
            fx.roots["reference_root"].write_bytes(b"not a directory")
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                build_promoted_operational_input_inventory(fx.control())

    def test_unsupported_entry_type_under_root_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fifo_path = fx.roots["reference_root"] / "weird"
            if hasattr(os, "mkfifo"):
                os.mkfifo(fifo_path)
                with self.assertRaises(PromotedOperationalInputSnapshotError):
                    build_promoted_operational_input_inventory(fx.control())
            else:
                self.skipTest("os.mkfifo unavailable on this platform")

    def test_missing_assembly_spec_file_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.assembly_spec_file.unlink()
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                build_promoted_operational_input_inventory(fx.control())

    def test_traversing_or_relative_root_is_rejected_at_control_construction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with self.assertRaises(Exception):
                fx.control(reference_root=Path("relative-reference-root"))

    def test_overlapping_roots_are_rejected_at_control_construction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with self.assertRaises(Exception):
                fx.control(portfolio_artifact_root=fx.roots["reference_root"] / "nested")

    def test_file_count_ceiling_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            (fx.roots["reference_root"] / "y.txt").write_bytes(b"y")
            with mock.patch.object(snap_module, "MAXIMUM_INCLUDED_FILES", 2):
                with self.assertRaises(PromotedOperationalInputSnapshotError):
                    build_promoted_operational_input_inventory(fx.control())

    def test_total_bytes_ceiling_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x" * 100)
            with mock.patch.object(snap_module, "MAXIMUM_TOTAL_BYTES", 50):
                with self.assertRaises(PromotedOperationalInputSnapshotError):
                    build_promoted_operational_input_inventory(fx.control())

    def test_manifest_encoded_ceiling_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            inventory = build_promoted_operational_input_inventory(fx.control())
            with mock.patch.object(snap_module, "MAXIMUM_ENCODED_BYTES", 10):
                with self.assertRaises(PromotedOperationalInputSnapshotError):
                    encode_promoted_operational_input_inventory(inventory)

    def test_assembly_spec_file_mutation_after_initial_read_is_detected(self) -> None:
        # The assembly-spec file is read once up front, then independently
        # re-lstat'd and compared at the very end of the scan window (after
        # every root has been walked and every directory fingerprint
        # re-verified) -- inject the mutation exactly in that window via
        # the first directory-fingerprint re-check.
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")

            original_verify = snap_module._verify_directory_unchanged
            state = {"done": False}

            def _mutate_then_verify(path, identity, names):
                if not state["done"]:
                    state["done"] = True
                    fx.assembly_spec_file.write_bytes(b"mutated content after initial read")
                return original_verify(path, identity, names)

            with mock.patch.object(
                snap_module, "_verify_directory_unchanged", side_effect=_mutate_then_verify
            ):
                with self.assertRaises(PromotedOperationalInputSnapshotError):
                    build_promoted_operational_input_inventory(fx.control())

    def test_state_root_content_is_never_scanned_or_represented(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.state_root.mkdir(parents=True, exist_ok=True)
            (fx.state_root / "secret.txt").write_bytes(b"must never be scanned")
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            inventory = build_promoted_operational_input_inventory(fx.control())
            for entry in inventory.entries:
                self.assertNotEqual(entry.input_name, "state_root")
            encoded_text = encode_promoted_operational_input_inventory(inventory).decode("utf-8")
            self.assertNotIn("state_root", encoded_text)
            self.assertNotIn("secret.txt", encoded_text)


class PathMappingTests(unittest.TestCase):
    def test_input_destination_path_maps_file_and_root_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control = fx.control()
            file_entry = PromotedOperationalInputEntry(
                input_name=FILE_INPUT_NAME, relative_path="", byte_count=1, sha256="a" * 64,
            )
            self.assertEqual(input_destination_path(control, file_entry), control.assembly_spec_file)

            root_entry = PromotedOperationalInputEntry(
                input_name="reference_root", relative_path="sub/x.txt", byte_count=1, sha256="a" * 64,
            )
            self.assertEqual(
                input_destination_path(control, root_entry), control.reference_root / "sub" / "x.txt"
            )

    def test_verify_hydrated_input_inventory_accepts_matching_and_rejects_mismatched(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "x.txt").write_bytes(b"x")
            inventory = build_promoted_operational_input_inventory(fx.control())
            verify_hydrated_input_inventory(fx.control(), inventory)

            (fx.roots["reference_root"] / "y.txt").write_bytes(b"extra")
            with self.assertRaises(PromotedOperationalInputSnapshotError):
                verify_hydrated_input_inventory(fx.control(), inventory)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_gcs_env_clock_network_or_kite_capability(self) -> None:
        source = inspect.getsource(snap_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "socket", "subprocess", "requests", "urllib", "httpx", "google",
            "kiteconnect", "time", "threading", "asyncio", "tempfile", "glob",
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
            "os.environ", "getenv(", "datetime.now(", "sleep(", "list_blobs(",
            "list_objects(", "storage.client(", "create_or_verify", "read_generation",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_defines_no_gcs_client_types(self) -> None:
        source = inspect.getsource(snap_module)
        self.assertNotIn("StateObjectWriter", source)
        self.assertNotIn("GCSObjectReader", source)


if __name__ == "__main__":
    unittest.main()
