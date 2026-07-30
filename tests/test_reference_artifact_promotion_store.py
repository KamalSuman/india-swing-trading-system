from __future__ import annotations

import base64
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.acquisition_promotion import (
    ReferenceArtifactPromotionService,
    VerifiedReferenceArtifactPromotion,
)
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.promotion_store import (
    LocalReferenceArtifactPromotionStore,
    ReferenceArtifactPromotionStoreConflict,
    ReferenceArtifactPromotionStoreError,
    ReferenceArtifactPromotionStoreNotFound,
    decode_promotion_manifest,
    encode_promotion_manifest,
)
from tests.test_reference_acquisition_promotion import (
    _build_pair,
    _security_master_gzip,
    _security_master_row,
    _join_for,
    _import_artifact,
)


UTC = timezone.utc


def _write_canonical_json(path: Path, value: dict) -> None:
    """Write canonical JSON bytes with an explicit single trailing LF.

    Path.write_text on Windows translates "\\n" to CRLF by default, which
    would make every mutation fixture below fail at this store's own
    generic noncanonical-byte check before ever exercising the intended
    field-specific rejection. write_bytes with an explicit UTF-8 encode
    sidesteps any newline translation entirely.
    """

    payload = (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8"
    )
    assert payload.endswith(b"\n") and b"\r" not in payload
    path.write_bytes(payload)


class ReferenceArtifactPromotionStoreAcceptanceTests(unittest.TestCase):
    def test_put_get_and_reinstantiation_replay_exact_equality(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")

            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            put_result = store.put(promotion)
            self.assertEqual(put_result, promotion)

            got = store.get(promotion.promotion_id)
            self.assertEqual(got, promotion)
            got.verify_content_identity()

            # Simulate a process restart: brand-new store instance, brand-new
            # LocalReferenceArtifactStore instance, no shared in-memory state.
            fresh_artifacts = LocalReferenceArtifactStore(root / "archive")
            fresh_store = LocalReferenceArtifactPromotionStore(
                root / "promotions", fresh_artifacts
            )
            replayed = fresh_store.get(promotion.promotion_id)
            self.assertEqual(replayed, promotion)

    def test_no_gcp_or_network_reader_is_ever_supplied(self) -> None:
        # LocalReferenceArtifactPromotionStore never accepts an injected GCS
        # reader at all -- it only ever constructs its own private
        # in-process reader bound to the sealed artifact's own raw bytes.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            store.get(promotion.promotion_id)
            import inspect

            signature = inspect.signature(LocalReferenceArtifactPromotionStore.__init__)
            self.assertNotIn("reader", signature.parameters)
            self.assertNotIn("client", signature.parameters)

    def test_idempotent_put_with_exact_existing_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            first = store.put(promotion)
            second = store.put(promotion)
            self.assertEqual(first, second)
            self.assertEqual(first, promotion)


class ReferenceArtifactPromotionStoreRejectionTests(unittest.TestCase):
    def _store(self, root: Path):
        artifacts = LocalReferenceArtifactStore(root / "archive")
        return LocalReferenceArtifactPromotionStore(root / "promotions", artifacts), artifacts

    def test_invalid_promotion_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, _ = self._store(root)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get("not-a-sha256")

    def test_path_traversal_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, _ = self._store(root)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get("../../etc/passwd" + "0" * 40)

    def test_missing_promotion_is_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, _ = self._store(root)
            with self.assertRaises(ReferenceArtifactPromotionStoreNotFound):
                store.get("0" * 64)

    def test_put_with_wrong_type_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, _ = self._store(root)
            with self.assertRaises(TypeError):
                store.put("not-a-promotion")  # type: ignore[arg-type]

    def test_missing_artifact_leaves_no_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a = Path(tmp_a)
            join, artifact = _build_pair(root_a)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)

            root_b = Path(tmp_b)
            # A store whose artifact archive never received this artifact.
            empty_artifacts = LocalReferenceArtifactStore(root_b / "archive")
            store = LocalReferenceArtifactPromotionStore(root_b / "promotions", empty_artifacts)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.put(promotion)
            self.assertFalse(store.path_for(promotion.promotion_id).exists())

    def test_wrong_artifact_resolution_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            # Import a second, different artifact into the same archive so
            # the promotion's own artifact_id still resolves correctly, but
            # verify that a store bound to an archive containing a
            # DIFFERENT sole artifact for that ID would fail: simulate this
            # by tampering with the retained promotion's own artifact
            # manifest fields before put (dataclasses are frozen, so this
            # uses a Path-only variant instead: point artifact_id-independent
            # resolution mismatch via a store whose archive has never seen
            # this exact artifact content).
            other_root = root / "other"
            gz_bytes = _security_master_gzip(
                rows=[_security_master_row(FinInstrmId="55555", TckrSymb="INFY")]
            )
            other_join = _join_for(gz_bytes, generation=555)
            other_artifact = _import_artifact(other_root, gz_bytes)
            other_promotion = ReferenceArtifactPromotionService().promote(
                other_join, other_artifact
            )
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.put(other_promotion)

    def test_tampered_receipt_bytes_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            tampered = bytearray(base64.b64decode(value["receipt_bytes_base64"]))
            tampered[0] ^= 0xFF
            value["receipt_bytes_base64"] = base64.b64encode(bytes(tampered)).decode("ascii")
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_tampered_binding_field_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["binding"]["allowed_bucket"] = "another-syntactically-valid-bucket"
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_widened_binding_cutoff_fails_under_the_pinned_promotion_id(self) -> None:
        # Blocker 2 regression: widening TrustedReferenceAcquisitionBinding.
        # cutoff by one day -- while every other field, and the receipt
        # itself, remain otherwise individually valid -- must not be
        # silently reconstructed under the original promotion_id. The v2
        # upstream promotion identity content-binds every trusted binding
        # field (including cutoff), so the independently recomputed
        # promotion_id now differs from the pinned path/record ID and get()
        # fails closed instead of accepting the widened trust boundary.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            original_cutoff = datetime.fromisoformat(value["binding"]["cutoff"])
            widened_cutoff = original_cutoff + timedelta(days=1)
            self.assertNotEqual(widened_cutoff, original_cutoff)
            value["binding"]["cutoff"] = widened_cutoff.isoformat()
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_tampered_artifact_id_projection_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["artifact_id"] = "f" * 64
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_tampered_manifest_id_projection_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["manifest_id"] = "e" * 64
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_tampered_raw_sha256_projection_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["raw_sha256"] = "d" * 64
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_tampered_join_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["join_id"] = "c" * 64
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_tampered_promotion_id_path_identity_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["promotion_id"] = "b" * 64
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_duplicate_keys_are_rejected(self) -> None:
        payload = (
            '{"artifact_id":"' + "a" * 64 + '","artifact_id":"' + "a" * 64 + '"}'
        ).encode("utf-8")
        with self.assertRaises(ReferenceArtifactPromotionStoreError):
            decode_promotion_manifest(payload)

    def test_unknown_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["unexpected_extra_field"] = "x"
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_float_token_is_rejected(self) -> None:
        payload = b'{"promotion_id": 1.5}'
        with self.assertRaises(ReferenceArtifactPromotionStoreError):
            decode_promotion_manifest(payload)

    def test_malformed_base64_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["receipt_bytes_base64"] = "not-valid-base64!!"
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_noncanonical_base64_padding_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            # Insert an equivalent-but-noncanonical base64 spelling (extra
            # trailing padding byte swapped) to ensure only exactly one
            # canonical spelling round-trips.
            original = value["receipt_bytes_base64"]
            value["receipt_bytes_base64"] = original[:-1] + (
                "A" if original[-1] != "A" else "B"
            )
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_malformed_date_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["binding"]["target_report_date"] = "16-07-2026"
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_malformed_datetime_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["binding"]["cutoff"] = "2026-07-16 23:59:59"
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_malformed_hash_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            value = json.loads(path.read_text("utf-8"))
            value["binding"]["trusted_acquirer_id"] = "not-a-hash"
            _write_canonical_json(path, value)
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_noncanonical_json_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            decoded = json.loads(path.read_text("utf-8"))
            path.write_bytes(json.dumps(decoded, indent=2, sort_keys=True).encode("utf-8"))
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_oversized_payload_is_rejected(self) -> None:
        payload = b'{"padding": "' + b"a" * (300 * 1024) + b'"}'
        with self.assertRaises(ReferenceArtifactPromotionStoreError):
            decode_promotion_manifest(payload)

    def test_empty_payload_is_rejected(self) -> None:
        with self.assertRaises(ReferenceArtifactPromotionStoreError):
            decode_promotion_manifest(b"")

    def test_symlinked_target_is_rejected_when_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            store.put(promotion)
            path = store.path_for(promotion.promotion_id)
            real = path.with_suffix(".real.json")
            path.rename(real)
            try:
                path.symlink_to(real)
            except OSError:
                self.skipTest("symbolic links are unavailable in this environment")
            with self.assertRaises(ReferenceArtifactPromotionStoreError):
                store.get(promotion.promotion_id)

    def test_create_once_conflict_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            artifacts = LocalReferenceArtifactStore(root / "archive")
            store = LocalReferenceArtifactPromotionStore(root / "promotions", artifacts)
            path = store.path_for(promotion.promotion_id)
            path.parent.mkdir(parents=True)
            path.write_bytes(b"{}\n")
            with self.assertRaises(ReferenceArtifactPromotionStoreConflict):
                store.put(promotion)


class ReferenceArtifactPromotionStoreCapabilityTests(unittest.TestCase):
    def test_no_listing_latest_nearest_find_or_io_capability_exists(self) -> None:
        banned_substrings = (
            "list",
            "latest",
            "nearest",
            "find",
            "network",
            "gcp",
            "environ",
            "clock",
        )
        members = [
            name for name in dir(LocalReferenceArtifactPromotionStore) if not name.startswith("__")
        ]
        for name in members:
            lowered = name.lower()
            self.assertFalse(
                any(bad in lowered for bad in banned_substrings),
                f"LocalReferenceArtifactPromotionStore unexpectedly exposes {name!r}",
            )
        public_members = {name for name in members if not name.startswith("_")}
        self.assertEqual(public_members, {"path_for", "put", "get"})


class ReferenceArtifactPromotionStoreCompatibilityTests(unittest.TestCase):
    def test_satisfies_local_promoted_identity_intake_store_resolver(self) -> None:
        from india_swing.identity_registry.promoted_intake import (
            PromotedIdentityIntakeService,
        )
        from india_swing.promoted_graph_store import LocalPromotedIdentityIntakeStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join_a, artifact_a = _build_pair(root)
            join_b, artifact_b = _build_pair(
                root,
                report_date=date(2026, 7, 15),
                acquired_at="2026-07-15T13:30:00Z",
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)

            artifacts = LocalReferenceArtifactStore(root / "archive")
            promotion_store = LocalReferenceArtifactPromotionStore(
                root / "promotions", artifacts
            )
            promotion_store.put(promotion_a)
            promotion_store.put(promotion_b)

            intake_store = LocalPromotedIdentityIntakeStore(
                root / "intakes", promotion_store
            )
            expected_report_dates = tuple(
                sorted((promotion_a.verified_report_date, promotion_b.verified_report_date))
            )
            intake = PromotedIdentityIntakeService().materialize(
                promotions=(promotion_a, promotion_b),
                expected_report_dates=expected_report_dates,
                cutoff=datetime(2026, 7, 16, 14, 0, tzinfo=UTC),
            )
            put_result = intake_store.put(intake)
            self.assertEqual(put_result, intake)

            # Simulate a process restart: brand-new store instances, sharing
            # no in-memory state, resolving purely through the durable
            # promotion store's own get(promotion_id).
            fresh_artifacts = LocalReferenceArtifactStore(root / "archive")
            fresh_promotion_store = LocalReferenceArtifactPromotionStore(
                root / "promotions", fresh_artifacts
            )
            fresh_intake_store = LocalPromotedIdentityIntakeStore(
                root / "intakes", fresh_promotion_store
            )
            self.assertEqual(fresh_intake_store.get(intake.intake_id), intake)


if __name__ == "__main__":
    unittest.main()
