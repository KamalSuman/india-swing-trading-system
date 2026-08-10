from __future__ import annotations

import ast
import inspect
import json
import unittest
from datetime import date
from pathlib import Path

import india_swing.promoted_operational_cloud_control as cc_module
from india_swing.promoted_operational_cloud_control import (
    MAXIMUM_CLOUD_CONTROL_BYTES,
    PROMOTED_OPERATIONAL_CLOUD_CONTROL_SCHEMA_VERSION,
    PromotedOperationalCloudControlError,
    PromotedOperationalCloudRunControl,
    decode_promoted_operational_cloud_control,
    encode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_gcs_state import PromotedOperationalGCSRestoreRequest

_SPEC_ID = "a" * 64
_ASSEMBLY_ID = "b" * 64
_BUCKET = "cloud-control-test-bucket"
_TARGET_SESSION = date(2026, 7, 17)


def _roots(base: Path) -> dict[str, Path]:
    names = (
        "reference_root",
        "identity_evidence_root",
        "calendar_root",
        "daily_reports_root",
        "historical_corpus_root",
        "promoted_root",
        "graph_publication_root",
        "engine_run_root",
        "research_run_root",
        "operational_preparation_root",
        "portfolio_artifact_root",
        "state_root",
    )
    return {name: base / name.replace("_", "-") for name in names}


def _control_kwargs(base: Path, *, prior_state_restore=None) -> dict[str, object]:
    kwargs: dict[str, object] = dict(
        expected_assembly_spec_id=_ASSEMBLY_ID,
        expected_operational_run_spec_id=_SPEC_ID,
        target_session=_TARGET_SESSION,
        state_bucket=_BUCKET,
        assembly_spec_file=base / "assembly.json",
        prior_state_restore=prior_state_restore,
    )
    kwargs.update(_roots(base))
    return kwargs


def _restore_request(base: Path) -> PromotedOperationalGCSRestoreRequest:
    return PromotedOperationalGCSRestoreRequest(
        bucket=_BUCKET,
        manifest_object_name=(
            f"promoted-operational-state/v1/2026-07-16/{_SPEC_ID}/manifests/{'c' * 64}.json"
        ),
        generation=1,
        expected_sha256="d" * 64,
        expected_spec_id=_SPEC_ID,
    )


class ControlCodecTests(unittest.TestCase):
    def test_round_trip_without_restore(self) -> None:
        base = Path("/tmp/cloud-control-a").resolve()
        control = PromotedOperationalCloudRunControl(**_control_kwargs(base))
        encoded = encode_promoted_operational_cloud_control(control)
        reloaded = decode_promoted_operational_cloud_control(encoded)
        self.assertEqual(reloaded.control_id, control.control_id)
        self.assertIsNone(reloaded.prior_state_restore)
        self.assertEqual(encode_promoted_operational_cloud_control(reloaded), encoded)

    def test_round_trip_with_restore(self) -> None:
        base = Path("/tmp/cloud-control-b").resolve()
        restore = _restore_request(base)
        control = PromotedOperationalCloudRunControl(**_control_kwargs(base, prior_state_restore=restore))
        encoded = encode_promoted_operational_cloud_control(control)
        reloaded = decode_promoted_operational_cloud_control(encoded)
        self.assertEqual(reloaded.control_id, control.control_id)
        self.assertEqual(reloaded.prior_state_restore.manifest_object_name, restore.manifest_object_name)
        self.assertEqual(encode_promoted_operational_cloud_control(reloaded), encoded)

    def test_control_id_is_deterministic_across_rebuilds(self) -> None:
        base = Path("/tmp/cloud-control-c").resolve()
        control1 = PromotedOperationalCloudRunControl(**_control_kwargs(base))
        control2 = PromotedOperationalCloudRunControl(**_control_kwargs(base))
        self.assertEqual(control1.control_id, control2.control_id)

    def test_decode_rejects_duplicate_keys(self) -> None:
        base = Path("/tmp/cloud-control-d").resolve()
        control = PromotedOperationalCloudRunControl(**_control_kwargs(base))
        encoded = encode_promoted_operational_cloud_control(control)
        text = encoded.decode("utf-8")
        tampered = text[:-2] + f',"state_bucket":"{_BUCKET}"}}\n'
        with self.assertRaises(PromotedOperationalCloudControlError):
            decode_promoted_operational_cloud_control(tampered.encode("utf-8"))

    def test_decode_rejects_unknown_keys(self) -> None:
        base = Path("/tmp/cloud-control-e").resolve()
        control = PromotedOperationalCloudRunControl(**_control_kwargs(base))
        encoded = encode_promoted_operational_cloud_control(control)
        text = encoded.decode("utf-8")[:-2] + ',"extra_field":"x"}\n'
        with self.assertRaises(PromotedOperationalCloudControlError):
            decode_promoted_operational_cloud_control(text.encode("utf-8"))

    def test_decode_rejects_missing_keys(self) -> None:
        base = Path("/tmp/cloud-control-f").resolve()
        control = PromotedOperationalCloudRunControl(**_control_kwargs(base))
        encoded = encode_promoted_operational_cloud_control(control)
        root = json.loads(encoded.decode("utf-8"))
        del root["cloud_control"]["state_bucket"]
        tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        with self.assertRaises(PromotedOperationalCloudControlError):
            decode_promoted_operational_cloud_control(tampered)

    def test_decode_rejects_float_literal(self) -> None:
        base = Path("/tmp/cloud-control-g").resolve()
        control = PromotedOperationalCloudRunControl(**_control_kwargs(base))
        encoded = encode_promoted_operational_cloud_control(control)
        text = encoded.decode("utf-8").replace('"control_id"', '"generation_marker":1.0,"control_id"')
        with self.assertRaises(PromotedOperationalCloudControlError):
            decode_promoted_operational_cloud_control(text.encode("utf-8"))

    def test_decode_rejects_invalid_utf8(self) -> None:
        with self.assertRaises(PromotedOperationalCloudControlError):
            decode_promoted_operational_cloud_control(b"\xff\xfe not utf-8")

    def test_decode_rejects_noncanonical_json(self) -> None:
        base = Path("/tmp/cloud-control-h").resolve()
        control = PromotedOperationalCloudRunControl(**_control_kwargs(base))
        encoded = encode_promoted_operational_cloud_control(control)
        pretty = encoded.decode("utf-8").replace(",", ", ")
        with self.assertRaises(PromotedOperationalCloudControlError):
            decode_promoted_operational_cloud_control(pretty.encode("utf-8"))

    def test_decode_rejects_empty_and_oversized_bytes(self) -> None:
        with self.assertRaises(PromotedOperationalCloudControlError):
            decode_promoted_operational_cloud_control(b"")
        with self.assertRaises(PromotedOperationalCloudControlError):
            decode_promoted_operational_cloud_control(b"{}" + b" " * MAXIMUM_CLOUD_CONTROL_BYTES)

    def test_rejects_subclassed_path(self) -> None:
        class _SubclassedPath(type(Path())):
            pass

        base = Path("/tmp/cloud-control-i").resolve()
        kwargs = _control_kwargs(base)
        kwargs["state_root"] = _SubclassedPath(kwargs["state_root"])
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_relative_path(self) -> None:
        base = Path("/tmp/cloud-control-j").resolve()
        kwargs = _control_kwargs(base)
        kwargs["state_root"] = Path("relative-state-root")
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_traversing_path(self) -> None:
        base = Path("/tmp/cloud-control-k").resolve()
        kwargs = _control_kwargs(base)
        kwargs["state_root"] = base / ".." / "escape"
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_overlapping_paths(self) -> None:
        base = Path("/tmp/cloud-control-l").resolve()
        kwargs = _control_kwargs(base)
        kwargs["portfolio_artifact_root"] = kwargs["state_root"] / "nested"
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_equal_paths(self) -> None:
        base = Path("/tmp/cloud-control-m").resolve()
        kwargs = _control_kwargs(base)
        kwargs["portfolio_artifact_root"] = kwargs["state_root"]
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_malformed_assembly_spec_id(self) -> None:
        base = Path("/tmp/cloud-control-n").resolve()
        kwargs = _control_kwargs(base)
        kwargs["expected_assembly_spec_id"] = "not-a-sha256"
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_malformed_bucket(self) -> None:
        base = Path("/tmp/cloud-control-o").resolve()
        kwargs = _control_kwargs(base)
        kwargs["state_bucket"] = "AB"
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_malformed_date(self) -> None:
        base = Path("/tmp/cloud-control-p").resolve()
        kwargs = _control_kwargs(base)
        kwargs["target_session"] = "2026-07-17"
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_restore_bucket_mismatch(self) -> None:
        base = Path("/tmp/cloud-control-q").resolve()
        restore = PromotedOperationalGCSRestoreRequest(
            bucket="a-different-bucket",
            manifest_object_name=f"promoted-operational-state/v1/2026-07-16/{_SPEC_ID}/manifests/{'c' * 64}.json",
            generation=1,
            expected_sha256="d" * 64,
            expected_spec_id=_SPEC_ID,
        )
        kwargs = _control_kwargs(base, prior_state_restore=restore)
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_restore_spec_mismatch(self) -> None:
        base = Path("/tmp/cloud-control-r").resolve()
        restore = PromotedOperationalGCSRestoreRequest(
            bucket=_BUCKET,
            manifest_object_name=f"promoted-operational-state/v1/2026-07-16/{'e' * 64}/manifests/{'c' * 64}.json",
            generation=1,
            expected_sha256="d" * 64,
            expected_spec_id="e" * 64,
        )
        kwargs = _control_kwargs(base, prior_state_restore=restore)
        with self.assertRaises(PromotedOperationalCloudControlError):
            PromotedOperationalCloudRunControl(**kwargs)

    def test_rejects_bool_generation_in_restore(self) -> None:
        base = Path("/tmp/cloud-control-s").resolve()
        with self.assertRaises(Exception):
            PromotedOperationalGCSRestoreRequest(
                bucket=_BUCKET,
                manifest_object_name=f"promoted-operational-state/v1/2026-07-16/{_SPEC_ID}/manifests/{'c' * 64}.json",
                generation=True,
                expected_sha256="d" * 64,
                expected_spec_id=_SPEC_ID,
            )


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_filesystem_env_clock_network_or_gcp_capability(self) -> None:
        source = inspect.getsource(cc_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "socket", "subprocess", "requests", "urllib", "httpx",
            "google", "kiteconnect", "time", "threading", "asyncio", "tempfile", "glob",
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
            "os.environ", "getenv(", "datetime.now(", "sleep(", "list_blobs(", "list_objects(",
            "open(", "read_stable_regular_file", "storage.client(",
        ):
            self.assertNotIn(token, lowered, msg=token)


if __name__ == "__main__":
    unittest.main()
