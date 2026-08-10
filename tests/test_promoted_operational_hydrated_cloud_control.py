from __future__ import annotations

import ast
import inspect
import json
import unittest
from datetime import date

import india_swing.promoted_operational_hydrated_cloud_control as hcc_module
from india_swing.promoted_operational_gcs_state import PromotedOperationalGCSRestoreRequest
from india_swing.promoted_operational_hydrated_cloud_control import (
    MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES,
    PROMOTED_OPERATIONAL_HYDRATED_CLOUD_LAUNCH_SCHEMA_VERSION,
    PromotedOperationalHydratedCloudLaunch,
    PromotedOperationalHydratedCloudLaunchError,
    decode_promoted_operational_hydrated_cloud_launch,
    encode_promoted_operational_hydrated_cloud_launch,
)
from india_swing.promoted_operational_input_gcs import PromotedOperationalInputRestoreRequest

_ASSEMBLY_ID = "a" * 64
_SPEC_ID = "b" * 64
_BUCKET = "hydrated-launch-test-bucket"
_TARGET_SESSION = date(2026, 7, 17)


def _input_restore(
    *,
    assembly_id: str = _ASSEMBLY_ID,
    bucket: str = _BUCKET,
    target_session: date = _TARGET_SESSION,
    snapshot_id: str = "c" * 64,
) -> PromotedOperationalInputRestoreRequest:
    return PromotedOperationalInputRestoreRequest(
        bucket=bucket,
        manifest_object_name=(
            f"promoted-operational-input/v1/{target_session.isoformat()}/{assembly_id}/"
            f"manifests/{snapshot_id}.json"
        ),
        generation=1,
        expected_sha256="d" * 64,
        expected_snapshot_id=snapshot_id,
        expected_assembly_spec_id=assembly_id,
        target_session=target_session,
    )


def _prior_restore(
    *, bucket: str = _BUCKET, spec_id: str = _SPEC_ID,
) -> PromotedOperationalGCSRestoreRequest:
    return PromotedOperationalGCSRestoreRequest(
        bucket=bucket,
        manifest_object_name=f"promoted-operational-state/v1/2026-07-16/{spec_id}/manifests/{'e' * 64}.json",
        generation=1,
        expected_sha256="f" * 64,
        expected_spec_id=spec_id,
    )


def _launch_kwargs(*, prior_state_restore=None) -> dict[str, object]:
    return dict(
        expected_assembly_spec_id=_ASSEMBLY_ID,
        expected_operational_run_spec_id=_SPEC_ID,
        target_session=_TARGET_SESSION,
        state_bucket=_BUCKET,
        input_restore=_input_restore(),
        prior_state_restore=prior_state_restore,
    )


class LaunchCodecTests(unittest.TestCase):
    def test_round_trip_without_prior_restore(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        reloaded = decode_promoted_operational_hydrated_cloud_launch(encoded)
        self.assertEqual(reloaded.launch_id, launch.launch_id)
        self.assertIsNone(reloaded.prior_state_restore)
        self.assertEqual(encode_promoted_operational_hydrated_cloud_launch(reloaded), encoded)

    def test_round_trip_with_prior_restore(self) -> None:
        prior = _prior_restore()
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs(prior_state_restore=prior))
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        reloaded = decode_promoted_operational_hydrated_cloud_launch(encoded)
        self.assertEqual(reloaded.launch_id, launch.launch_id)
        self.assertEqual(reloaded.prior_state_restore.manifest_object_name, prior.manifest_object_name)
        self.assertEqual(encode_promoted_operational_hydrated_cloud_launch(reloaded), encoded)

    def test_launch_id_is_deterministic_across_rebuilds(self) -> None:
        launch1 = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        launch2 = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        self.assertEqual(launch1.launch_id, launch2.launch_id)

    def test_input_restore_is_a_freshly_reconstructed_detached_instance(self) -> None:
        original_input_restore = _input_restore()
        launch = PromotedOperationalHydratedCloudLaunch(
            expected_assembly_spec_id=_ASSEMBLY_ID,
            expected_operational_run_spec_id=_SPEC_ID,
            target_session=_TARGET_SESSION,
            state_bucket=_BUCKET,
            input_restore=original_input_restore,
        )
        self.assertIsNot(launch.input_restore, original_input_restore)
        self.assertEqual(launch.input_restore.manifest_object_name, original_input_restore.manifest_object_name)

    def test_decode_rejects_duplicate_keys(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        text = encoded.decode("utf-8")
        tampered = text[:-2] + f',"state_bucket":"{_BUCKET}"}}\n'
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(tampered.encode("utf-8"))

    def test_decode_rejects_unknown_keys(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        text = encoded.decode("utf-8")[:-2] + ',"extra_field":"x"}\n'
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(text.encode("utf-8"))

    def test_decode_rejects_missing_keys(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        root = json.loads(encoded.decode("utf-8"))
        del root["hydrated_cloud_launch"]["state_bucket"]
        tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(tampered)

    def test_decode_rejects_nested_missing_keys(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        root = json.loads(encoded.decode("utf-8"))
        del root["hydrated_cloud_launch"]["input_restore"]["bucket"]
        tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(tampered)

    def test_decode_rejects_nested_extra_keys(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(
            **_launch_kwargs(prior_state_restore=_prior_restore())
        )
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        root = json.loads(encoded.decode("utf-8"))
        root["hydrated_cloud_launch"]["prior_state_restore"]["extra"] = "x"
        tampered = (json.dumps(root, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(tampered)

    def test_decode_rejects_float_literal(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        text = encoded.decode("utf-8").replace('"launch_id"', '"generation_marker":1.0,"launch_id"')
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(text.encode("utf-8"))

    def test_decode_rejects_nan_literal(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        text = encoded.decode("utf-8").replace('"launch_id"', '"generation_marker":NaN,"launch_id"')
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(text.encode("utf-8"))

    def test_decode_rejects_invalid_utf8(self) -> None:
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(b"\xff\xfe not utf-8")

    def test_decode_rejects_noncanonical_json(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        encoded = encode_promoted_operational_hydrated_cloud_launch(launch)
        pretty = encoded.decode("utf-8").replace(",", ", ")
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(pretty.encode("utf-8"))

    def test_decode_rejects_empty_and_oversized_bytes(self) -> None:
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(b"")
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            decode_promoted_operational_hydrated_cloud_launch(b"{}" + b" " * MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES)

    def test_rejects_wrong_schema_version(self) -> None:
        kwargs = _launch_kwargs()
        kwargs["schema_version"] = "wrong-schema/v1"
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_str_subclass_schema_version_equal_to_the_constant(self) -> None:
        class _StrSubclass(str):
            pass

        kwargs = _launch_kwargs()
        kwargs["schema_version"] = _StrSubclass(PROMOTED_OPERATIONAL_HYDRATED_CLOUD_LAUNCH_SCHEMA_VERSION)
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_malformed_assembly_spec_id(self) -> None:
        kwargs = _launch_kwargs()
        kwargs["expected_assembly_spec_id"] = "not-a-sha256"
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_malformed_bucket(self) -> None:
        kwargs = _launch_kwargs()
        kwargs["state_bucket"] = "AB"
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_malformed_target_session(self) -> None:
        kwargs = _launch_kwargs()
        kwargs["target_session"] = "2026-07-17"
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_subclassed_expected_assembly_spec_id(self) -> None:
        class _StrSubclass(str):
            pass

        kwargs = _launch_kwargs()
        kwargs["expected_assembly_spec_id"] = _StrSubclass(_ASSEMBLY_ID)
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_input_restore_of_wrong_type(self) -> None:
        kwargs = _launch_kwargs()
        kwargs["input_restore"] = "not-a-request"
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_input_restore_bucket_mismatch(self) -> None:
        kwargs = _launch_kwargs()
        kwargs["input_restore"] = _input_restore(bucket="a-different-bucket")
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_input_restore_assembly_mismatch(self) -> None:
        kwargs = _launch_kwargs()
        kwargs["input_restore"] = _input_restore(assembly_id="e" * 64)
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_input_restore_session_mismatch(self) -> None:
        kwargs = _launch_kwargs()
        kwargs["input_restore"] = _input_restore(target_session=date(2026, 7, 18))
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_prior_restore_bucket_mismatch(self) -> None:
        kwargs = _launch_kwargs(prior_state_restore=_prior_restore(bucket="a-different-bucket"))
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_prior_restore_spec_mismatch(self) -> None:
        kwargs = _launch_kwargs(prior_state_restore=_prior_restore(spec_id="e" * 64))
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            PromotedOperationalHydratedCloudLaunch(**kwargs)

    def test_rejects_bool_generation_in_input_restore(self) -> None:
        with self.assertRaises(Exception):
            PromotedOperationalInputRestoreRequest(
                bucket=_BUCKET,
                manifest_object_name=(
                    f"promoted-operational-input/v1/{_TARGET_SESSION.isoformat()}/{_ASSEMBLY_ID}/"
                    f"manifests/{'c' * 64}.json"
                ),
                generation=True,
                expected_sha256="d" * 64,
                expected_snapshot_id="c" * 64,
                expected_assembly_spec_id=_ASSEMBLY_ID,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_bool_generation_in_prior_restore(self) -> None:
        with self.assertRaises(Exception):
            PromotedOperationalGCSRestoreRequest(
                bucket=_BUCKET,
                manifest_object_name=f"promoted-operational-state/v1/2026-07-16/{_SPEC_ID}/manifests/{'e' * 64}.json",
                generation=True,
                expected_sha256="f" * 64,
                expected_spec_id=_SPEC_ID,
            )

    def test_post_construction_mutation_is_detected_by_verify_content_identity(self) -> None:
        launch = PromotedOperationalHydratedCloudLaunch(**_launch_kwargs())
        object.__setattr__(launch, "state_bucket", "a-different-bucket")
        with self.assertRaises(PromotedOperationalHydratedCloudLaunchError):
            launch.verify_content_identity()


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_filesystem_env_clock_network_or_gcp_capability(self) -> None:
        source = inspect.getsource(hcc_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os",
            "pathlib",
            "socket",
            "subprocess",
            "requests",
            "urllib",
            "httpx",
            "google",
            "kiteconnect",
            "time",
            "threading",
            "asyncio",
            "tempfile",
            "glob",
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
            "os.environ",
            "getenv(",
            "datetime.now(",
            "sleep(",
            "list_blobs(",
            "list_objects(",
            "open(",
            "read_stable_regular_file",
            "storage.client(",
            "pathlib.path",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_launch_type_has_no_path_field(self) -> None:
        for field_name, field_type in PromotedOperationalHydratedCloudLaunch.__annotations__.items():
            self.assertNotIn("Path", str(field_type), msg=field_name)


if __name__ == "__main__":
    unittest.main()
