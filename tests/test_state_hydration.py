from __future__ import annotations

import ast
import hashlib
import unittest
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline.state_blob_acquisition import (
    AcquiredStateBlob,
    VerifiedPipelineStateBlobs,
    acquire_verified_pipeline_state_blobs,
)
from india_swing.daily_pipeline.state_hydration import (
    HydratedPipelineStateEntry,
    PipelineStateHydrationError,
    VerifiedHydratedPipelineState,
    hydrate_verified_pipeline_state,
)
from india_swing.daily_pipeline.state_inventory import PipelineStateEntry

from tests.test_state_blob_acquisition import _build_control, _reader_for


_MODULE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "india_swing"
    / "daily_pipeline"
    / "state_hydration.py"
)


def _acquired() -> tuple[VerifiedPipelineStateBlobs, dict[str, bytes]]:
    control, contents = _build_control()
    return acquire_verified_pipeline_state_blobs(
        control,
        reader=_reader_for(control, contents),
    ), contents


class SuccessTests(unittest.TestCase):
    def test_hydrates_every_inventory_entry_in_canonical_order(self) -> None:
        acquired, contents = _acquired()

        result = hydrate_verified_pipeline_state(acquired)

        expected = acquired.control.inventory.entries
        self.assertIs(type(result), VerifiedHydratedPipelineState)
        self.assertIsNot(result.acquired_blobs, acquired)
        self.assertEqual(
            tuple(item.inventory_entry for item in result.entries),
            expected,
        )
        self.assertEqual(
            tuple(item.content_bytes for item in result.entries),
            tuple(contents[item.sha256] for item in expected),
        )
        self.assertEqual(
            sum(len(item.content_bytes) for item in result.entries),
            acquired.control.inventory.total_bytes,
        )

    def test_duplicate_inventory_hash_reuses_one_immutable_bytes_object(self) -> None:
        acquired, _ = _acquired()
        result = hydrate_verified_pipeline_state(acquired)
        self.assertEqual(
            result.entries[0].inventory_entry.sha256,
            result.entries[1].inventory_entry.sha256,
        )
        self.assertIs(result.entries[0].content_bytes, result.entries[1].content_bytes)

    def test_empty_verified_state_hydrates_to_empty_tuple(self) -> None:
        control, _ = _build_control(entries=(), blob_objects=())
        acquired = VerifiedPipelineStateBlobs(control=control, blobs=())
        result = hydrate_verified_pipeline_state(acquired)
        self.assertEqual(result.entries, ())
        self.assertEqual(result.acquired_blobs.control.inventory.total_bytes, 0)


class InputStrictnessTests(unittest.TestCase):
    def test_rejects_nonexact_acquired_blob_aggregate(self) -> None:
        with self.assertRaisesRegex(
            PipelineStateHydrationError,
            "^pipeline state hydration blob verification failed$",
        ):
            hydrate_verified_pipeline_state(object())  # type: ignore[arg-type]

    def test_rejects_subclassed_acquired_blob_aggregate(self) -> None:
        class SubclassedBlobs(VerifiedPipelineStateBlobs):
            pass

        acquired, _ = _acquired()
        subclassed = SubclassedBlobs(
            control=acquired.control,
            blobs=acquired.blobs,
        )
        with self.assertRaises(PipelineStateHydrationError):
            hydrate_verified_pipeline_state(subclassed)

    def test_rejects_mutated_acquired_blob_aggregate(self) -> None:
        acquired, _ = _acquired()
        object.__setattr__(acquired, "blobs", acquired.blobs[:-1])
        with self.assertRaisesRegex(
            PipelineStateHydrationError,
            "^pipeline state hydration blob verification failed$",
        ):
            hydrate_verified_pipeline_state(acquired)

    def test_sanitizes_nested_verification_error(self) -> None:
        acquired, _ = _acquired()
        secret = "do-not-leak-this-value"
        object.__setattr__(acquired.control.request, "bucket", secret)
        with self.assertRaises(PipelineStateHydrationError) as caught:
            hydrate_verified_pipeline_state(acquired)
        self.assertEqual(
            str(caught.exception),
            "pipeline state hydration blob verification failed",
        )
        self.assertNotIn(secret, str(caught.exception))


class EntryStrictnessTests(unittest.TestCase):
    def test_entry_defensively_reconstructs_inventory_metadata(self) -> None:
        acquired, _ = _acquired()
        original = acquired.control.inventory.entries[0]
        content = next(
            item.content_bytes
            for item in acquired.blobs
            if item.published_object.sha256 == original.sha256
        )
        item = HydratedPipelineStateEntry(original, content)
        self.assertIsNot(item.inventory_entry, original)

    def test_entry_rejects_subclassed_inventory_entry(self) -> None:
        class SubclassedEntry(PipelineStateEntry):
            pass

        acquired, _ = _acquired()
        original = acquired.control.inventory.entries[0]
        subclassed = SubclassedEntry(
            original.root_name,
            original.relative_path,
            original.byte_count,
            original.sha256,
        )
        with self.assertRaises(PipelineStateHydrationError):
            HydratedPipelineStateEntry(subclassed, acquired.blobs[0].content_bytes)

    def test_entry_rejects_nonbytes_content(self) -> None:
        acquired, _ = _acquired()
        with self.assertRaises(PipelineStateHydrationError):
            HydratedPipelineStateEntry(
                acquired.control.inventory.entries[0],
                bytearray(acquired.blobs[0].content_bytes),  # type: ignore[arg-type]
            )

    def test_entry_rejects_wrong_content(self) -> None:
        acquired, _ = _acquired()
        with self.assertRaises(PipelineStateHydrationError):
            HydratedPipelineStateEntry(
                acquired.control.inventory.entries[0],
                b"wrong-content",
            )


class AggregateStrictnessTests(unittest.TestCase):
    def test_rejects_non_tuple_entries(self) -> None:
        acquired, _ = _acquired()
        hydrated = hydrate_verified_pipeline_state(acquired)
        with self.assertRaises(PipelineStateHydrationError):
            VerifiedHydratedPipelineState(
                acquired_blobs=acquired,
                entries=list(hydrated.entries),  # type: ignore[arg-type]
            )

    def test_rejects_missing_extra_reordered_or_duplicate_entries(self) -> None:
        acquired, _ = _acquired()
        hydrated = hydrate_verified_pipeline_state(acquired)
        candidates = (
            hydrated.entries[:-1],
            hydrated.entries + (hydrated.entries[-1],),
            tuple(reversed(hydrated.entries)),
            (hydrated.entries[0],) + hydrated.entries,
        )
        for entries in candidates:
            with self.subTest(length=len(entries)):
                with self.assertRaises(PipelineStateHydrationError):
                    VerifiedHydratedPipelineState(acquired, entries)

    def test_rejects_subclassed_hydrated_entry(self) -> None:
        class SubclassedHydratedEntry(HydratedPipelineStateEntry):
            pass

        acquired, _ = _acquired()
        hydrated = hydrate_verified_pipeline_state(acquired)
        original = hydrated.entries[0]
        subclassed = SubclassedHydratedEntry(
            original.inventory_entry,
            original.content_bytes,
        )
        with self.assertRaises(PipelineStateHydrationError):
            VerifiedHydratedPipelineState(
                acquired,
                (subclassed,) + hydrated.entries[1:],
            )

    def test_rejects_mutated_hydrated_entry(self) -> None:
        acquired, _ = _acquired()
        hydrated = hydrate_verified_pipeline_state(acquired)
        object.__setattr__(hydrated.entries[0], "content_bytes", b"tampered")
        with self.assertRaises(PipelineStateHydrationError):
            VerifiedHydratedPipelineState(acquired, hydrated.entries)

    def test_accepts_equal_distinct_bytes_objects(self) -> None:
        acquired, _ = _acquired()
        hydrated = hydrate_verified_pipeline_state(acquired)
        copied = tuple(
            HydratedPipelineStateEntry(
                item.inventory_entry,
                bytes(bytearray(item.content_bytes)),
            )
            for item in hydrated.entries
        )
        result = VerifiedHydratedPipelineState(acquired, copied)
        self.assertEqual(result.entries, copied)


class FailureBoundaryTests(unittest.TestCase):
    def test_ordinary_hash_failure_is_sanitized(self) -> None:
        acquired, _ = _acquired()
        with patch(
            "india_swing.daily_pipeline.state_hydration.hashlib.sha256",
            side_effect=RuntimeError("secret internal failure"),
        ):
            with self.assertRaises(PipelineStateHydrationError) as caught:
                hydrate_verified_pipeline_state(acquired)
        self.assertEqual(
            str(caught.exception),
            "pipeline state hydration blob verification failed",
        )
        self.assertNotIn("secret", str(caught.exception))

    def test_base_exception_is_not_swallowed(self) -> None:
        acquired, _ = _acquired()
        with patch(
            "india_swing.daily_pipeline.state_hydration.hashlib.sha256",
            side_effect=KeyboardInterrupt,
        ):
            with self.assertRaises(KeyboardInterrupt):
                hydrate_verified_pipeline_state(acquired)


class CapabilityLockTests(unittest.TestCase):
    _EXACT_ALLOWED_IMPORTS = frozenset(
        {
            (0, "__future__", "annotations", None),
            (0, "hashlib", None, None),
            (0, "dataclasses", "dataclass", None),
            (1, "state_blob_acquisition", "VerifiedPipelineStateBlobs", None),
            (1, "state_inventory", "PipelineStateEntry", None),
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
        "broker",
        "place_order",
        "submit_order",
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
