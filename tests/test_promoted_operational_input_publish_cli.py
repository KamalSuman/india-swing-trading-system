from __future__ import annotations

import ast
import hashlib
import inspect
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_input_publish_cli as publish_cli_module
from india_swing.promoted_operational_assembly import encode_promoted_operational_assembly_spec
from india_swing.promoted_operational_cloud_control import (
    PromotedOperationalCloudRunControl,
    encode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_gcs_state import PromotedOperationalGCSRestoreRequest
from india_swing.promoted_operational_hydrated_cloud_control import (
    decode_promoted_operational_hydrated_cloud_launch,
)
from india_swing.promoted_operational_input_gcs import CompletedPromotedOperationalInputPublication
from india_swing.promoted_operational_input_publish_cli import main
from india_swing.promoted_operational_input_snapshot import ROOT_INPUT_NAMES

from tests import test_promoted_operational_job as _job_tests


class _FakeBlob:
    def __init__(self, store, bucket_name, object_name, requested_generation):
        self._store = store
        self._key = (bucket_name, object_name)
        self.name = object_name
        self.requested_generation = requested_generation
        self.generation = requested_generation

    def upload_from_string(self, content_bytes, *, content_type, if_generation_match, checksum, retry):
        existing = self._store.get(self._key)
        if existing is not None:
            if existing[0] == content_bytes:
                self.generation = existing[1]
                return
            raise RuntimeError("fake GCS conflict: differing content at existing path")
        generation = len(self._store) + 1
        self._store[self._key] = (content_bytes, generation)
        self.generation = generation

    def reload(self, *, retry=None):
        existing = self._store.get(self._key)
        if existing is None:
            raise LookupError("not found")
        self.generation = existing[1]


class _FakeBucket:
    def __init__(self, store, calls, name):
        self._store = store
        self._calls = calls
        self.name = name

    def blob(self, object_name, generation=None):
        self._calls.append(object_name)
        return _FakeBlob(self._store, self.name, object_name, generation)


class _FakeGCSClient:
    def __init__(self) -> None:
        self._store: dict[tuple[str, str], tuple[bytes, int]] = {}
        self.bucket_calls: list[str] = []
        self.blob_calls: list[str] = []

    def bucket(self, name):
        self.bucket_calls.append(name)
        return _FakeBucket(self._store, self.blob_calls, name)


def _roots(base: Path) -> dict[str, Path]:
    return {name: base / name.replace("_", "-") for name in ROOT_INPUT_NAMES}


class _Fixture:
    def __init__(self, tmp_path: Path, *, open_positions: int = 0) -> None:
        self.tmp_path = tmp_path
        tmp_path.mkdir(parents=True, exist_ok=True)
        _preparation, _run_spec, _portfolio, _artifact, self.spec = _job_tests._fixture(
            open_positions=open_positions
        )
        self.assembly_spec_file = tmp_path / "assembly.json"
        self.assembly_spec_file.write_bytes(encode_promoted_operational_assembly_spec(self.spec))
        self.state_root = tmp_path / "state"
        self.roots = _roots(tmp_path / "src")
        for path in self.roots.values():
            path.mkdir(parents=True, exist_ok=True)
        self.client = _FakeGCSClient()

    def control(self, *, prior_state_restore=None) -> PromotedOperationalCloudRunControl:
        return PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=self.spec.assembly_spec_id,
            expected_operational_run_spec_id="b" * 64,
            target_session=self.spec.target_session,
            state_bucket=self.spec.binding_bucket,
            assembly_spec_file=self.assembly_spec_file,
            state_root=self.state_root,
            prior_state_restore=prior_state_restore,
            **self.roots,
        )

    def control_file(self, control: PromotedOperationalCloudRunControl | None = None, *, name: str = "control.json") -> Path:
        if control is None:
            control = self.control()
        path = self.tmp_path / name
        path.write_bytes(encode_promoted_operational_cloud_control(control))
        return path

    def output_launch_file(self, *, name: str = "launch.json") -> Path:
        return self.tmp_path / name

    def run(self, argv, **kwargs):
        common: dict[str, object] = dict(gcs_client_factory=lambda: self.client)
        common.update(kwargs)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(argv, **common)
        return code, stdout.getvalue(), stderr.getvalue()

    def publish(self, **kwargs):
        control_file = self.control_file()
        output_file = self.output_launch_file()
        code, out, err = self.run(
            ["--source-control-file", str(control_file), "--output-launch-file", str(output_file)], **kwargs
        )
        return code, out, err, output_file


class PublisherTests(unittest.TestCase):
    def test_success_publishes_snapshot_and_writes_launch_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            code, out, err, output_file = fx.publish()
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            self.assertTrue(output_file.exists())
            result = json.loads(out)
            self.assertEqual(result["status"], "PROMOTED_OPERATIONAL_INPUT_LAUNCH_READY")
            launch = decode_promoted_operational_hydrated_cloud_launch(output_file.read_bytes())
            self.assertEqual(result["launch_id"], launch.launch_id)
            self.assertEqual(result["input_snapshot_id"], launch.input_restore.expected_snapshot_id)
            self.assertEqual(result["input_manifest_object_name"], launch.input_restore.manifest_object_name)
            self.assertEqual(result["input_manifest_generation"], launch.input_restore.generation)
            self.assertEqual(result["input_manifest_sha256"], launch.input_restore.expected_sha256)
            self.assertEqual(
                set(result),
                {
                    "status", "launch_id", "input_snapshot_id", "expected_assembly_spec_id",
                    "expected_operational_run_spec_id", "target_session", "input_manifest_object_name",
                    "input_manifest_generation", "input_manifest_sha256", "input_manifest_byte_count",
                },
            )

    def test_exact_restore_request_derivation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err, output_file = fx.publish()
            self.assertEqual(code, 0)
            result = json.loads(out)
            launch = decode_promoted_operational_hydrated_cloud_launch(output_file.read_bytes())
            self.assertEqual(launch.input_restore.bucket, fx.spec.binding_bucket)
            self.assertEqual(launch.input_restore.expected_assembly_spec_id, fx.spec.assembly_spec_id)
            self.assertEqual(launch.input_restore.target_session, fx.spec.target_session)
            self.assertEqual(launch.input_restore.expected_snapshot_id, result["input_snapshot_id"])

    def test_prior_state_restore_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            prior = PromotedOperationalGCSRestoreRequest(
                bucket=fx.spec.binding_bucket,
                manifest_object_name=f"promoted-operational-state/v1/2026-07-16/{'b' * 64}/manifests/{'c' * 64}.json",
                generation=1,
                expected_sha256="d" * 64,
                expected_spec_id="b" * 64,
            )
            control_file = fx.control_file(fx.control(prior_state_restore=prior))
            output_file = fx.output_launch_file()
            code, out, err = fx.run(
                ["--source-control-file", str(control_file), "--output-launch-file", str(output_file)]
            )
            self.assertEqual(code, 0)
            launch = decode_promoted_operational_hydrated_cloud_launch(output_file.read_bytes())
            self.assertIsNotNone(launch.prior_state_restore)
            self.assertEqual(launch.prior_state_restore.manifest_object_name, prior.manifest_object_name)

    def test_source_control_and_assembly_validation_precede_client_construction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            wrong_control = PromotedOperationalCloudRunControl(
                expected_assembly_spec_id="9" * 64,
                expected_operational_run_spec_id="b" * 64,
                target_session=fx.spec.target_session,
                state_bucket=fx.spec.binding_bucket,
                assembly_spec_file=fx.assembly_spec_file,
                state_root=fx.state_root,
                **fx.roots,
            )
            control_file = fx.control_file(wrong_control)
            output_file = fx.output_launch_file()
            gcs_calls: list[int] = []
            code, out, err = fx.run(
                ["--source-control-file", str(control_file), "--output-launch-file", str(output_file)],
                gcs_client_factory=lambda: gcs_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(gcs_calls, [])
            self.assertFalse(output_file.exists())

    def test_missing_control_file_fails_before_client_construction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            output_file = fx.output_launch_file()
            gcs_calls: list[int] = []
            code, out, err = fx.run(
                ["--source-control-file", str(fx.tmp_path / "missing.json"), "--output-launch-file", str(output_file)],
                gcs_client_factory=lambda: gcs_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 2)
            self.assertEqual(gcs_calls, [])

    def test_exactly_one_client_and_one_publication_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            client_calls: list[int] = []
            original_publish = publish_cli_module.publish_promoted_operational_input_snapshot
            publish_calls: list[int] = []

            def _tracked_publish(**kwargs):
                publish_calls.append(1)
                return original_publish(**kwargs)

            with mock.patch.object(
                publish_cli_module, "publish_promoted_operational_input_snapshot", side_effect=_tracked_publish,
            ):
                code, out, err, output_file = fx.publish(
                    gcs_client_factory=lambda: client_calls.append(1) or fx.client,
                )
            self.assertEqual(code, 0)
            self.assertEqual(client_calls, [1])
            self.assertEqual(publish_calls, [1])

    def test_output_is_create_once_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            code1, out1, err1, output_file = fx.publish()
            self.assertEqual(code1, 0)
            first_bytes = output_file.read_bytes()

            control_file = fx.control_file()
            code2, out2, err2 = fx.run(
                ["--source-control-file", str(control_file), "--output-launch-file", str(output_file)]
            )
            self.assertEqual(code2, 0)
            self.assertEqual(output_file.read_bytes(), first_bytes)

    def test_divergent_existing_output_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            output_file = fx.output_launch_file()
            output_file.write_bytes(b"not a launch file")
            control_file = fx.control_file()
            code, out, err = fx.run(
                ["--source-control-file", str(control_file), "--output-launch-file", str(output_file)]
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(output_file.read_bytes(), b"not a launch file")

    def test_nonexistent_output_parent_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            output_file = fx.tmp_path / "missing-parent" / "launch.json"
            code, out, err = fx.run(
                ["--source-control-file", str(control_file), "--output-launch-file", str(output_file)]
            )
            self.assertEqual(code, 2)
            self.assertFalse(output_file.exists())

    def test_output_that_is_a_directory_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            output_dir = fx.output_launch_file()
            output_dir.mkdir()
            code, out, err = fx.run(
                ["--source-control-file", str(control_file), "--output-launch-file", str(output_dir)]
            )
            self.assertEqual(code, 2)

    def test_malicious_publication_return_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                publish_cli_module,
                "publish_promoted_operational_input_snapshot",
                return_value="not-a-completed-publication",
            ):
                code, out, err, output_file = fx.publish()
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertFalse(output_file.exists())

    def test_wrong_type_argument_combinations_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            output_file = fx.output_launch_file()

            with self.subTest(case="unknown_flag"):
                code, out, err = fx.run(
                    [
                        "--source-control-file", str(control_file), "--output-launch-file", str(output_file),
                        "--extra", "x",
                    ]
                )
                self.assertEqual(code, 2)

            with self.subTest(case="relative_source"):
                code, out, err = fx.run(
                    ["--source-control-file", "relative.json", "--output-launch-file", str(output_file)]
                )
                self.assertEqual(code, 2)

            with self.subTest(case="traversing_output"):
                code, out, err = fx.run(
                    [
                        "--source-control-file", str(control_file),
                        "--output-launch-file", str(fx.tmp_path / ".." / "escape.json"),
                    ]
                )
                self.assertEqual(code, 2)

            with self.subTest(case="missing_output_flag"):
                code, out, err = fx.run(["--source-control-file", str(control_file)])
                self.assertEqual(code, 2)


class ParentIdentityReplacementTests(unittest.TestCase):
    """Reproduces the exact revision-1 gap: the originally validated empty
    output parent is renamed and a different directory created at the
    same path between initial validation and the write. Each test drives
    the specific recheck call-site via a call-counted side_effect wrapper
    around the real ``_verify_parent_identity`` (raising only on the
    exact call index named, delegating to the real implementation
    otherwise) -- a physical filesystem race would be flaky and platform-
    dependent on Windows. Every scenario must fail closed with no success,
    no overwrite, and no cleanup."""

    def _fail_on_call(self, index: int):
        original = publish_cli_module._verify_parent_identity
        calls = {"count": 0}

        def _wrapper(parent, expected_identity):
            calls["count"] += 1
            if calls["count"] == index:
                raise publish_cli_module.PromotedOperationalInputPublishError("simulated parent replacement")
            return original(parent, expected_identity)

        return _wrapper, calls

    def test_replacement_before_exclusive_creation_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            wrapper, calls = self._fail_on_call(1)
            with mock.patch.object(publish_cli_module, "_verify_parent_identity", side_effect=wrapper):
                code, out, err, output_file = fx.publish()
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertFalse(output_file.exists())
            self.assertEqual(calls["count"], 1)
            self.assertEqual(
                json.loads(err), {"error_type": "PromotedOperationalInputPublishError", "status": "FAILED"},
            )

    def test_replacement_before_replay_read_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            # First establish a byte-identical existing output so the
            # replay-read branch (not the fresh-write branch) is taken.
            code0, out0, err0, output_file = fx.publish()
            self.assertEqual(code0, 0)
            original_bytes = output_file.read_bytes()

            wrapper, calls = self._fail_on_call(1)
            control_file = fx.control_file()
            with mock.patch.object(publish_cli_module, "_verify_parent_identity", side_effect=wrapper):
                code, out, err = fx.run(
                    ["--source-control-file", str(control_file), "--output-launch-file", str(output_file)]
                )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(calls["count"], 1)
            # No overwrite/cleanup: the pre-existing file is left exactly
            # as it was.
            self.assertEqual(output_file.read_bytes(), original_bytes)

    def test_replacement_after_cold_read_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            wrapper, calls = self._fail_on_call(2)
            with mock.patch.object(publish_cli_module, "_verify_parent_identity", side_effect=wrapper):
                code, out, err, output_file = fx.publish()
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(calls["count"], 2)
            # The write and cold-read both already succeeded before the
            # final recheck fails -- no cleanup/deletion is attempted, so
            # the file is left in place even though the overall call
            # reports failure.
            self.assertTrue(output_file.exists())


class SanitizationTests(unittest.TestCase):
    SECRET_MARKER = "SUPER-SECRET-PUBLISH-CLI-MARKER-9j2d"

    def test_writer_exception_containing_secret_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())

            def _raising_factory():
                raise RuntimeError(f"leaked token={self.SECRET_MARKER}")

            code, out, err, output_file = fx.publish(gcs_client_factory=_raising_factory)
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(
                json.loads(err), {"error_type": "PromotedOperationalInputPublishError", "status": "FAILED"},
            )
            self.assertNotIn(self.SECRET_MARKER, err)


class PyprojectRegistrationTests(unittest.TestCase):
    def test_both_console_script_mappings_are_exact(self) -> None:
        pyproject_path = (
            Path(inspect.getfile(publish_cli_module)).resolve().parent.parent.parent / "pyproject.toml"
        )
        text = pyproject_path.read_text(encoding="utf-8")
        self.assertIn(
            'india-swing-promoted-operational-input-publish = '
            '"india_swing.promoted_operational_input_publish_cli:main"',
            text,
        )
        self.assertIn(
            'india-swing-promoted-operational-hydrated-cloud-job = '
            '"india_swing.promoted_operational_hydrated_cloud_job:main"',
            text,
        )


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_forbidden_capability(self) -> None:
        source = inspect.getsource(publish_cli_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "socket", "subprocess", "requests", "urllib", "httpx",
            "kiteconnect", "time", "threading", "asyncio", "glob", "shutil",
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
            "list_blobs(", "list_objects(", "place_order(",
            "modify_order(", "cancel_order(", "telegram", "kite", "sleep(",
            "gcloud", "gsutil", "docker",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_never_dereferences_state_root(self) -> None:
        source = inspect.getsource(publish_cli_module)
        self.assertNotIn("state_root", source)

    def test_module_does_not_import_a_concrete_gcs_sdk_client_at_top_level(self) -> None:
        source = inspect.getsource(publish_cli_module)
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.Import):
                self.assertFalse(any(alias.name.split(".")[0] == "google" for alias in node.names))
            elif isinstance(node, ast.ImportFrom) and node.module:
                self.assertNotEqual(node.module.split(".")[0], "google")


if __name__ == "__main__":
    unittest.main()
