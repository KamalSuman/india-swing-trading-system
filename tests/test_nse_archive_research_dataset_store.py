from __future__ import annotations

import ast
import inspect
import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from india_swing._filesystem import advisory_file_lock
from india_swing.evaluation import nse_archive_research_dataset_store
from india_swing.evaluation.nse_archive_research_dataset_store import (
    MAXIMUM_MANIFEST_BYTES,
    LocalNseArchiveResearchDatasetStore,
    NseArchiveResearchDatasetError,
    NseArchiveResearchDatasetStoreConflict,
    NseArchiveResearchDatasetStoreNotFound,
    decode_nse_archive_research_dataset,
    encode_nse_archive_research_dataset,
)
from tests.test_nse_archive_research_dataset import (
    _GAP_DAY,
    ResearchArchiveExclusion,
    ResearchArchiveExclusionReason,
    _baseline_dataset,
)


def _fully_populated_dataset():
    return _baseline_dataset(
        gap_before_validation=True,
        exclusions=(
            ResearchArchiveExclusion(
                _GAP_DAY, ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED
            ),
        ),
    )


def _payload_dict(dataset) -> dict:
    return json.loads(encode_nse_archive_research_dataset(dataset).decode("utf-8"))


def _encode_dict(payload: dict) -> bytes:
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


class CodecRoundTripTests(unittest.TestCase):
    def test_round_trip_is_byte_deterministic_and_exactly_equal(self) -> None:
        dataset = _fully_populated_dataset()
        first = encode_nse_archive_research_dataset(dataset)
        second = encode_nse_archive_research_dataset(dataset)
        self.assertEqual(first, second)

        reconstructed = decode_nse_archive_research_dataset(first)
        self.assertEqual(reconstructed, dataset)
        self.assertEqual(reconstructed.dataset_id, dataset.dataset_id)
        self.assertEqual(reconstructed.index_snapshot_ids, dataset.index_snapshot_ids)
        self.assertEqual(reconstructed.split_policy, dataset.split_policy)
        self.assertEqual(reconstructed.split_policy_id, dataset.split_policy_id)
        self.assertEqual(
            tuple((value.session, value.reason) for value in reconstructed.exclusions),
            tuple((value.session, value.reason) for value in dataset.exclusions),
        )
        self.assertEqual(
            reconstructed.evidence_profile_counts, dataset.evidence_profile_counts
        )
        self.assertEqual(
            tuple(value.role for value in reconstructed.partitions),
            tuple(value.role for value in dataset.partitions),
        )
        for flag in (
            "collection_only",
            "actionable",
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "alert_eligible",
            "execution_eligible",
            "identity_resolution_complete",
            "corporate_action_adjustment_complete",
            "coverage_complete",
        ):
            self.assertEqual(getattr(reconstructed, flag), getattr(dataset, flag))
        reconstructed.verify_content_identity()


class CodecRejectionTests(unittest.TestCase):
    def _assert_sanitized_rejection(self, payload: bytes, *, message: str) -> None:
        with self.assertRaises(NseArchiveResearchDatasetStoreConflict) as ctx:
            decode_nse_archive_research_dataset(payload)
        self.assertEqual(str(ctx.exception), message)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

    def test_reject_empty_and_non_bytes_and_invalid_utf8(self) -> None:
        with self.subTest("empty"):
            self._assert_sanitized_rejection(
                b"", message="stored research dataset is invalid"
            )
        with self.subTest("non_bytes"):
            with self.assertRaises(NseArchiveResearchDatasetStoreConflict) as ctx:
                decode_nse_archive_research_dataset("not-bytes")  # type: ignore[arg-type]
            self.assertEqual(
                str(ctx.exception), "stored research dataset is invalid"
            )
        with self.subTest("invalid_utf8"):
            self._assert_sanitized_rejection(
                b"\xff\xfe\x00\x01", message="stored research dataset is invalid"
            )

    def test_reject_oversized_payload(self) -> None:
        oversized = b"x" * (MAXIMUM_MANIFEST_BYTES + 1)
        self._assert_sanitized_rejection(
            oversized, message="stored research dataset exceeds its size limit"
        )

    def test_reject_duplicate_keys_at_top_level_and_nested(self) -> None:
        dataset = _fully_populated_dataset()
        canonical = encode_nse_archive_research_dataset(dataset).decode("utf-8")

        with self.subTest("top_level"):
            text = canonical.replace(
                '"dataset_id":', '"dataset_id":"dup","dataset_id":', 1
            )
            self._assert_sanitized_rejection(
                text.encode("utf-8"), message="stored research dataset is invalid"
            )

        with self.subTest("nested_in_split_policy"):
            text = canonical.replace(
                '"policy_id":', '"policy_id":"dup","policy_id":', 1
            )
            self._assert_sanitized_rejection(
                text.encode("utf-8"), message="stored research dataset is invalid"
            )

    def test_reject_floats_nan_and_infinity(self) -> None:
        dataset = _fully_populated_dataset()
        canonical = encode_nse_archive_research_dataset(dataset).decode("utf-8")

        with self.subTest("float"):
            text = canonical.replace('"record_count":63', '"record_count":63.0', 1)
            self._assert_sanitized_rejection(
                text.encode("utf-8"), message="stored research dataset is invalid"
            )

        with self.subTest("nan"):
            text = canonical.replace('"record_count":63', '"record_count":NaN', 1)
            self._assert_sanitized_rejection(
                text.encode("utf-8"), message="stored research dataset is invalid"
            )

        with self.subTest("infinity"):
            text = canonical.replace(
                '"record_count":63', '"record_count":Infinity', 1
            )
            self._assert_sanitized_rejection(
                text.encode("utf-8"), message="stored research dataset is invalid"
            )

    def test_reject_missing_extra_and_reordered_fields(self) -> None:
        dataset = _fully_populated_dataset()

        with self.subTest("missing_root_field"):
            payload = _payload_dict(dataset)
            del payload["record_count"]
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("extra_root_field"):
            payload = _payload_dict(dataset)
            payload["unexpected_field"] = 1
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("missing_nested_field"):
            payload = _payload_dict(dataset)
            del payload["split_policy"]["train_end"]
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("reordered_keys_fail_canonical_re_encoding"):
            payload = _payload_dict(dataset)
            reordered = dict(reversed(list(payload.items())))
            text = (
                json.dumps(
                    reordered,
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=False,
                )
                + "\n"
            )
            # Content is identical and every claimed ID still matches its
            # reconstruction, so this is rejected only by the canonical
            # re-encoding equality check, not by any structural/identity check.
            self._assert_sanitized_rejection(
                text.encode("utf-8"), message="stored research dataset is invalid"
            )

    def test_reject_invalid_enum_date_int_bool_sha_and_schema_values(self) -> None:
        dataset = _fully_populated_dataset()

        with self.subTest("invalid_enum_reason"):
            payload = _payload_dict(dataset)
            payload["exclusions"][0]["reason"] = "NOT_A_REAL_REASON"
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("invalid_enum_role"):
            payload = _payload_dict(dataset)
            payload["partitions"][0]["role"] = "NOT_A_ROLE"
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("invalid_date"):
            payload = _payload_dict(dataset)
            payload["split_policy"]["train_end"] = "13-99-9999"
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("invalid_int"):
            payload = _payload_dict(dataset)
            payload["record_count"] = "63"
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("invalid_bool"):
            payload = _payload_dict(dataset)
            payload["collection_only"] = "true"
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("invalid_sha"):
            payload = _payload_dict(dataset)
            payload["index_snapshot_ids"][0] = "not-a-sha"
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("invalid_schema"):
            payload = _payload_dict(dataset)
            payload["store_schema_version"] = "wrong-schema/v0"
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

    def test_reject_every_claimed_id_mismatch(self) -> None:
        dataset = _fully_populated_dataset()
        id_paths = {
            "binding_id": lambda payload: payload["range_bindings"][0],
            "exclusion_id": lambda payload: payload["exclusions"][0],
            "partition_id": lambda payload: payload["partitions"][0],
            "policy_id": lambda payload: payload["split_policy"],
            "dataset_id": lambda payload: payload,
            "split_policy_id": lambda payload: payload,
        }
        for id_field, locate in id_paths.items():
            with self.subTest(id_field):
                payload = _payload_dict(dataset)
                container = locate(payload)
                container[id_field] = (
                    "0" * 64 if container[id_field] != "0" * 64 else "1" * 64
                )
                self._assert_sanitized_rejection(
                    _encode_dict(payload),
                    message="stored research dataset is invalid",
                )

    def test_reject_any_safety_flag_upgrade(self) -> None:
        dataset = _fully_populated_dataset()
        for flag in (
            "collection_only",
            "actionable",
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "alert_eligible",
            "execution_eligible",
            "identity_resolution_complete",
            "corporate_action_adjustment_complete",
            "coverage_complete",
        ):
            with self.subTest(flag):
                payload = _payload_dict(dataset)
                payload[flag] = not payload[flag]
                self._assert_sanitized_rejection(
                    _encode_dict(payload),
                    message="stored research dataset is invalid",
                )

    def test_reject_malformed_partition_and_policy_boundary(self) -> None:
        dataset = _fully_populated_dataset()

        with self.subTest("policy_boundary_gap"):
            payload = _payload_dict(dataset)
            payload["split_policy"]["validation_start"] = (
                date.fromisoformat(payload["split_policy"]["validation_start"])
                .replace(day=1)
                .isoformat()
            )
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )

        with self.subTest("partition_horizon_too_low"):
            payload = _payload_dict(dataset)
            payload["partitions"][0]["maximum_forward_label_horizon_sessions"] = 1
            self._assert_sanitized_rejection(
                _encode_dict(payload), message="stored research dataset is invalid"
            )


class LocalNseArchiveResearchDatasetStoreTests(unittest.TestCase):
    def test_put_get_is_idempotent_and_create_once(self) -> None:
        dataset = _fully_populated_dataset()
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            first = store.put(dataset)
            second = store.put(dataset)
            self.assertEqual(first, dataset)
            self.assertEqual(second, dataset)
            self.assertEqual(store.get(dataset.dataset_id), dataset)

    def test_put_rejects_wrong_type(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            with self.assertRaises(TypeError):
                store.put("not-a-dataset")  # type: ignore[arg-type]

    def test_get_rejects_wrong_id_shape_including_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            for bad_id in (
                "not-a-sha",
                "A" * 64,
                "0" * 63,
                "../../../etc/passwd",
                "0" * 64 + "/../secret",
            ):
                with self.subTest(bad_id):
                    with self.assertRaises(NseArchiveResearchDatasetError):
                        store.get(bad_id)

    def test_get_missing_raises_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            with self.assertRaises(NseArchiveResearchDatasetStoreNotFound):
                store.get("0" * 64)

    def test_get_rejects_a_directory_target(self) -> None:
        dataset = _fully_populated_dataset()
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            store.dataset_root.mkdir(parents=True)
            (store.dataset_root / f"{dataset.dataset_id}.json").mkdir()
            with self.assertRaises(NseArchiveResearchDatasetStoreConflict):
                store.get(dataset.dataset_id)

    def test_get_rejects_an_unreadable_empty_artifact(self) -> None:
        dataset = _fully_populated_dataset()
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            store.dataset_root.mkdir(parents=True)
            (store.dataset_root / f"{dataset.dataset_id}.json").write_bytes(b"")
            with self.assertRaises(NseArchiveResearchDatasetStoreConflict):
                store.get(dataset.dataset_id)

    def test_get_rejects_an_oversized_artifact(self) -> None:
        dataset = _fully_populated_dataset()
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            store.dataset_root.mkdir(parents=True)
            (store.dataset_root / f"{dataset.dataset_id}.json").write_bytes(
                b"x" * (MAXIMUM_MANIFEST_BYTES + 1)
            )
            with self.assertRaises(NseArchiveResearchDatasetStoreConflict):
                store.get(dataset.dataset_id)

    def test_put_rejects_conflicting_content_at_the_same_target_path(self) -> None:
        dataset_a = _fully_populated_dataset()
        dataset_b = _fully_populated_dataset()
        # Give dataset_a a synthetic accepted-session shift so it is a
        # genuinely different dataset from dataset_b.
        from tests.test_nse_archive_research_dataset import _baseline_dataset as base

        dataset_a = base(gap_before_validation=True, shift_days=500)
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            store.dataset_root.mkdir(parents=True)
            # Store dataset_b's real bytes under dataset_a's target filename,
            # simulating a corrupted/conflicting store: the artifact decodes
            # fine on its own, but its content does not match the ID the
            # filename claims.
            target = store.dataset_root / f"{dataset_a.dataset_id}.json"
            target.write_bytes(encode_nse_archive_research_dataset(dataset_b))
            with self.assertRaises(NseArchiveResearchDatasetStoreConflict):
                store.put(dataset_a)

    def test_stray_temp_artifacts_do_not_affect_normal_operation(self) -> None:
        dataset = _fully_populated_dataset()
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            store.dataset_root.mkdir(parents=True)
            (store.dataset_root / ".nse-archive-research-dataset-stray.tmp").write_bytes(
                b"leftover from a crashed write"
            )
            store.put(dataset)
            self.assertEqual(store.get(dataset.dataset_id), dataset)

    def test_put_fails_closed_when_lock_is_already_held(self) -> None:
        dataset = _fully_populated_dataset()
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            store.dataset_root.mkdir(parents=True)
            lock_path = (
                store.dataset_root / ".nse-archive-research-datasets.lock"
            )
            with advisory_file_lock(lock_path):
                with self.assertRaises(NseArchiveResearchDatasetStoreConflict):
                    store.put(dataset)
            # The target must not have been created by the failed attempt.
            self.assertFalse(
                (store.dataset_root / f"{dataset.dataset_id}.json").exists()
            )

    def test_hardened_against_a_linked_dataset_root(self) -> None:
        dataset = _fully_populated_dataset()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            real_target = root / "elsewhere"
            real_target.mkdir()
            linked_root = root / "store"
            try:
                os.symlink(real_target, linked_root, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("symlinks are not permitted in this environment")
            store = LocalNseArchiveResearchDatasetStore(linked_root)
            with self.assertRaises(NseArchiveResearchDatasetStoreConflict):
                store.put(dataset)


class CapabilityTests(unittest.TestCase):
    def test_module_imports_are_limited_to_a_pure_offline_allowlist(self) -> None:
        source_path = inspect.getsourcefile(nse_archive_research_dataset_store)
        assert source_path is not None
        tree = ast.parse(Path(source_path).read_text(encoding="utf-8"))
        allowed_roots = {
            "__future__",
            "json",
            "os",
            "re",
            "stat",
            "tempfile",
            "datetime",
            "pathlib",
            "india_swing",
            "nse_archive_research_dataset",
        }
        imported_roots: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_roots.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imported_roots.add(node.module.split(".")[0])
        self.assertTrue(imported_roots.issubset(allowed_roots), imported_roots)

    def test_module_source_has_no_network_clock_broker_or_listing_tokens(self) -> None:
        source_path = inspect.getsourcefile(nse_archive_research_dataset_store)
        assert source_path is not None
        lowered = Path(source_path).read_text(encoding="utf-8").lower()
        forbidden_tokens = (
            "socket",
            "requests",
            "urllib",
            "http.client",
            "os.environ",
            "getenv",
            "time.time",
            "datetime.now",
            "utcnow",
            "subprocess",
            "telegram",
            "boto3",
            "google.cloud",
            "storage.client",
            ".glob(",
            "listdir",
            ".iterdir(",
        )
        for token in forbidden_tokens:
            self.assertNotIn(token, lowered, token)

    def test_store_exposes_only_put_and_get_no_list_latest_delete_or_update(
        self,
    ) -> None:
        public_attributes = {
            name
            for name in dir(LocalNseArchiveResearchDatasetStore)
            if not name.startswith("_")
        }
        # dataset_root is a plain read-only path property, not a capability;
        # the store's only callable operations must be put and get.
        self.assertEqual(public_attributes, {"put", "get", "dataset_root"})
        forbidden_names = (
            "list",
            "latest",
            "glob",
            "delete",
            "remove",
            "update",
            "overwrite",
            "alias",
        )
        for name in forbidden_names:
            self.assertNotIn(name, public_attributes)


class AtomicPublicationRaceTests(unittest.TestCase):
    """Regression coverage for Codex's rejected-revision-1 probe: a target
    created by another writer between our absence check and publication must
    never be overwritten."""

    def test_no_os_replace_reference_in_module_source(self) -> None:
        source_path = inspect.getsourcefile(nse_archive_research_dataset_store)
        assert source_path is not None
        source = Path(source_path).read_text(encoding="utf-8")
        # A call site would read "os.replace(...)"; the module's own put()
        # docstring mentions the bare phrase "os.replace" (no trailing paren)
        # to document why it is deliberately not used, which must not trip
        # this check.
        self.assertNotIn("os.replace(", source)

    def test_race_injecting_a_different_target_is_rejected_without_overwriting(
        self,
    ) -> None:
        dataset = _fully_populated_dataset()
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            real_link = os.link
            captured: dict[str, Path] = {}

            def racing_link(src, dst, *args, **kwargs):
                path = Path(dst)
                captured["target"] = path
                path.write_bytes(b"PREEXISTING_EVIDENCE")
                return real_link(src, dst, *args, **kwargs)

            with patch(
                "india_swing.evaluation.nse_archive_research_dataset_store.os.link",
                side_effect=racing_link,
            ):
                with self.assertRaises(NseArchiveResearchDatasetStoreConflict) as ctx:
                    store.put(dataset)

            self.assertIsNone(ctx.exception.__cause__)
            self.assertIsNone(ctx.exception.__context__)
            self.assertEqual(
                captured["target"].read_bytes(), b"PREEXISTING_EVIDENCE"
            )
            leftovers = [
                item
                for item in store.dataset_root.iterdir()
                if item.suffix == ".tmp" or item.name.endswith(".tmp")
            ]
            self.assertEqual(leftovers, [])

    def test_race_injecting_identical_canonical_bytes_is_idempotent(self) -> None:
        dataset = _fully_populated_dataset()
        canonical = encode_nse_archive_research_dataset(dataset)
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            real_link = os.link

            def racing_link(src, dst, *args, **kwargs):
                Path(dst).write_bytes(canonical)
                return real_link(src, dst, *args, **kwargs)

            with patch(
                "india_swing.evaluation.nse_archive_research_dataset_store.os.link",
                side_effect=racing_link,
            ):
                result = store.put(dataset)

            self.assertEqual(result, dataset)
            self.assertEqual(
                (store.dataset_root / f"{dataset.dataset_id}.json").read_bytes(),
                canonical,
            )
            leftovers = [
                item
                for item in store.dataset_root.iterdir()
                if item.suffix == ".tmp" or item.name.endswith(".tmp")
            ]
            self.assertEqual(leftovers, [])


class FaultInjectionBoundaryTests(unittest.TestCase):
    """Every OS/lock boundary put()/get() touch must fail closed behind one
    static sanitized error, with no planted secret, path, errno text, or
    exception cause/context ever escaping, and a prior valid artifact must
    survive every injected write-side failure byte-for-byte unchanged."""

    def _boundary_patchers(self, secret: str):
        module_path = "india_swing.evaluation.nse_archive_research_dataset_store"
        return {
            "mkdir": patch.object(Path, "mkdir", side_effect=OSError(secret)),
            "mkstemp": patch(
                f"{module_path}.tempfile.mkstemp", side_effect=OSError(secret)
            ),
            "fsync": patch(f"{module_path}.os.fsync", side_effect=OSError(secret)),
            "link": patch(f"{module_path}.os.link", side_effect=OSError(secret)),
            "exists": patch.object(Path, "exists", side_effect=OSError(secret)),
            "stat": patch.object(Path, "stat", side_effect=OSError(secret)),
            "stable_read": patch(
                f"{module_path}.read_stable_regular_file",
                side_effect=OSError(secret),
            ),
        }

    def test_put_contains_every_boundary_failure_without_leaking(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK"
        prior = _fully_populated_dataset()
        for index, (name, patcher) in enumerate(
            self._boundary_patchers(secret).items()
        ):
            with self.subTest(name):
                with tempfile.TemporaryDirectory() as temporary:
                    store = LocalNseArchiveResearchDatasetStore(Path(temporary))
                    store.put(prior)
                    prior_path = store.dataset_root / f"{prior.dataset_id}.json"
                    prior_bytes = prior_path.read_bytes()

                    candidate = _baseline_dataset(
                        gap_before_validation=True, shift_days=3000 + index
                    )
                    with patcher:
                        with self.assertRaises(
                            NseArchiveResearchDatasetStoreConflict
                        ) as ctx:
                            store.put(candidate)

                    message = str(ctx.exception)
                    self.assertNotIn(secret, message)
                    self.assertNotIn(secret, repr(ctx.exception))
                    self.assertIsNone(ctx.exception.__cause__)
                    self.assertIsNone(ctx.exception.__context__)
                    self.assertEqual(prior_path.read_bytes(), prior_bytes)
                    leftovers = [
                        item
                        for item in store.dataset_root.iterdir()
                        if item.name.endswith(".tmp")
                    ]
                    self.assertEqual(leftovers, [])

    def test_get_contains_boundary_failures_without_leaking(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK"
        dataset = _fully_populated_dataset()
        module_path = "india_swing.evaluation.nse_archive_research_dataset_store"
        get_boundaries = {
            "exists": patch.object(Path, "exists", side_effect=OSError(secret)),
            "stat": patch.object(Path, "stat", side_effect=OSError(secret)),
            "stable_read": patch(
                f"{module_path}.read_stable_regular_file",
                side_effect=OSError(secret),
            ),
        }
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalNseArchiveResearchDatasetStore(Path(temporary))
            store.put(dataset)
            for name, patcher in get_boundaries.items():
                with self.subTest(name):
                    with patcher:
                        with self.assertRaises(
                            NseArchiveResearchDatasetStoreConflict
                        ) as ctx:
                            store.get(dataset.dataset_id)
                    message = str(ctx.exception)
                    self.assertNotIn(secret, message)
                    self.assertNotIn(secret, repr(ctx.exception))
                    self.assertIsNone(ctx.exception.__cause__)
                    self.assertIsNone(ctx.exception.__context__)
            # Unpatched, the artifact is still fully intact and loadable.
            self.assertEqual(store.get(dataset.dataset_id), dataset)


if __name__ == "__main__":
    unittest.main()
