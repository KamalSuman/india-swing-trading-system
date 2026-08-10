from __future__ import annotations

import ast
import hashlib
import inspect
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_hydrated_cloud_job as hcj_module
from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.promoted_operational_assembly import encode_promoted_operational_assembly_spec
from india_swing.promoted_operational_cloud_control import (
    PromotedOperationalCloudRunControl,
    decode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_gcs_state import PromotedOperationalGCSRestoreRequest
from india_swing.promoted_operational_hydrated_cloud_control import (
    PromotedOperationalHydratedCloudLaunch,
    encode_promoted_operational_hydrated_cloud_launch,
)
from india_swing.promoted_operational_hydrated_cloud_job import (
    PromotedOperationalHydratedCloudJobError,
    main,
)
from india_swing.promoted_operational_input_gcs import (
    AcquiredPromotedOperationalInputSnapshot,
    CompletedPromotedOperationalInputRestore,
    PromotedOperationalInputRestoreRequest,
    publish_promoted_operational_input_snapshot,
)
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

    def download_as_bytes(self, *, end=None, raw_download=True, if_generation_match=None, retry=None):
        existing = self._store.get(self._key)
        if existing is None:
            raise LookupError("not found")
        content_bytes, stored_generation = existing
        if if_generation_match is not None and if_generation_match != stored_generation:
            raise LookupError("generation mismatch")
        self.generation = stored_generation
        if end is None:
            return content_bytes
        return content_bytes[: end + 1]


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


class _FakeCloudJobMain:
    """Records every call and lets a test dictate the inner
    ``promoted_operational_cloud_job.main``'s exact exit code / stdout /
    stderr, without running the real assembly/Kite/quote-gate/GCS-state
    pipeline -- used for the wrapper's own post-inner-job validation."""

    def __init__(self, *, exit_code: int = 0, stdout_text: str | None = None, stderr_text: str = "") -> None:
        self.exit_code = exit_code
        self.stdout_text = stdout_text
        self.stderr_text = stderr_text
        self.calls: list[dict[str, object]] = []

    def __call__(self, argv, **kwargs) -> int:
        control = decode_promoted_operational_cloud_control(Path(argv[1]).read_bytes())
        self.calls.append({"argv": list(argv), "kwargs": dict(kwargs), "control": control})
        if self.stdout_text is not None:
            sys.stdout.write(self.stdout_text)
        sys.stderr.write(self.stderr_text)
        return self.exit_code


def _roots(base: Path) -> dict[str, Path]:
    return {name: base / name.replace("_", "-") for name in ROOT_INPUT_NAMES}


def _expected_hydration_control(
    launch: PromotedOperationalHydratedCloudLaunch, parent: Path
) -> PromotedOperationalCloudRunControl:
    root_paths = {name: parent / name for name in ROOT_INPUT_NAMES}
    return PromotedOperationalCloudRunControl(
        expected_assembly_spec_id=launch.expected_assembly_spec_id,
        expected_operational_run_spec_id=launch.expected_operational_run_spec_id,
        target_session=launch.target_session,
        state_bucket=launch.state_bucket,
        assembly_spec_file=parent / "assembly-spec.json",
        state_root=parent / "state",
        prior_state_restore=launch.prior_state_restore,
        **root_paths,
    )


def _valid_envelope_body(
    launch: PromotedOperationalHydratedCloudLaunch, cloud_control_id: str, **overrides
) -> dict[str, object]:
    body: dict[str, object] = {
        "status": "PROMOTED_OPERATIONAL_JOB_COMPLETE",
        "assembly_spec_id": launch.expected_assembly_spec_id,
        "runtime_job_spec_id": "c" * 64,
        "operational_run_spec_id": launch.expected_operational_run_spec_id,
        "preparation_id": "d" * 64,
        "target_session": launch.target_session.isoformat(),
        "terminal_id": "e" * 64,
        "terminal_status": "COMPLETE",
        "action": "NO_TRADE",
        "failure_codes": [],
        "advisory_id": "f" * 64,
        "binding_id": "1" * 64,
        "binding_generation": 1,
        "reused_existing_terminal": False,
        "paper_only": True,
        "notification_eligible": False,
        "execution_eligible": False,
        "cloud_control_id": cloud_control_id,
        "state_publication_id": "2" * 64,
        "state_manifest_object_name": (
            f"promoted-operational-state/v1/{launch.target_session.isoformat()}/"
            f"{launch.expected_operational_run_spec_id}/manifests/{'2' * 64}.json"
        ),
        "state_manifest_generation": 1,
        "state_manifest_sha256": "3" * 64,
        "state_manifest_byte_count": 100,
    }
    body.update(overrides)
    return body


def _envelope_text(body: dict[str, object]) -> str:
    return json.dumps(body, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"


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

    def source_control(
        self, *, expected_operational_run_spec_id: str = "b" * 64, prior_state_restore=None,
    ) -> PromotedOperationalCloudRunControl:
        return PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=self.spec.assembly_spec_id,
            expected_operational_run_spec_id=expected_operational_run_spec_id,
            target_session=self.spec.target_session,
            state_bucket=self.spec.binding_bucket,
            assembly_spec_file=self.assembly_spec_file,
            state_root=self.state_root,
            prior_state_restore=prior_state_restore,
            **self.roots,
        )

    def launch(self, **kwargs) -> PromotedOperationalHydratedCloudLaunch:
        control = self.source_control(**kwargs)
        writer = GoogleCloudStorageStateObjectWriter(client=self.client)
        publication = publish_promoted_operational_input_snapshot(
            control=control, bucket=control.state_bucket, writer=writer,
        )
        input_restore = PromotedOperationalInputRestoreRequest(
            bucket=control.state_bucket,
            manifest_object_name=publication.manifest_object.object_name,
            generation=publication.manifest_object.generation,
            expected_sha256=publication.manifest_object.sha256,
            expected_snapshot_id=publication.manifest.snapshot_id,
            expected_assembly_spec_id=control.expected_assembly_spec_id,
            target_session=control.target_session,
        )
        return PromotedOperationalHydratedCloudLaunch(
            expected_assembly_spec_id=control.expected_assembly_spec_id,
            expected_operational_run_spec_id=control.expected_operational_run_spec_id,
            target_session=control.target_session,
            state_bucket=control.state_bucket,
            input_restore=input_restore,
            prior_state_restore=control.prior_state_restore,
        )

    def launch_file(self, launch: PromotedOperationalHydratedCloudLaunch | None = None, **kwargs):
        if launch is None:
            launch = self.launch(**kwargs)
        path = self.tmp_path / "launch.json"
        path.write_bytes(encode_promoted_operational_hydrated_cloud_launch(launch))
        return path, launch

    def runtime_parent(self, *, name: str = "runtime", create: bool = True) -> Path:
        path = self.tmp_path / name
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def run(self, argv, **kwargs):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(argv, **kwargs)
        return code, stdout.getvalue(), stderr.getvalue()

    def full_run(self, *, launch=None, cloud_job_main=None, runtime_parent=None, launch_kwargs=None, **kwargs):
        launch_file, launch = self.launch_file(launch=launch, **(launch_kwargs or {}))
        parent = runtime_parent if runtime_parent is not None else self.runtime_parent()
        if cloud_job_main is None:
            cloud_control_id = _expected_hydration_control(launch, parent).control_id
            cloud_job_main = _FakeCloudJobMain(stdout_text=_envelope_text(_valid_envelope_body(launch, cloud_control_id)))
        common: dict[str, object] = dict(
            runtime_parent=parent, gcs_client_factory=lambda: self.client, cloud_job_main=cloud_job_main,
        )
        common.update(kwargs)
        code, out, err = self.run(["--launch-file", str(launch_file)], **common)
        return code, out, err, launch, parent, cloud_job_main


class FreshPathTests(unittest.TestCase):
    def test_full_round_trip_hydrates_and_completes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"hello")
            code, out, err, launch, parent, inner = fx.full_run()
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            self.assertEqual(len(inner.calls), 1)

            result = json.loads(out)
            self.assertEqual(result["status"], "PROMOTED_OPERATIONAL_HYDRATED_CLOUD_JOB_COMPLETE")
            self.assertEqual(result["inner_status"], "PROMOTED_OPERATIONAL_JOB_COMPLETE")
            self.assertEqual(result["launch_id"], launch.launch_id)
            self.assertEqual(result["input_snapshot_id"], launch.input_restore.expected_snapshot_id)
            self.assertEqual(result["input_manifest_object_name"], launch.input_restore.manifest_object_name)
            self.assertEqual(result["input_manifest_generation"], launch.input_restore.generation)
            self.assertEqual(result["input_manifest_sha256"], launch.input_restore.expected_sha256)
            self.assertEqual(result["paper_only"], True)
            self.assertEqual(result["notification_eligible"], False)
            self.assertEqual(result["execution_eligible"], False)

            expected_keys = {
                "status", "assembly_spec_id", "runtime_job_spec_id", "operational_run_spec_id",
                "preparation_id", "target_session", "terminal_id", "terminal_status", "action",
                "failure_codes", "advisory_id", "binding_id", "binding_generation",
                "reused_existing_terminal", "paper_only", "notification_eligible", "execution_eligible",
                "cloud_control_id", "state_publication_id", "state_manifest_object_name",
                "state_manifest_generation", "state_manifest_sha256", "state_manifest_byte_count",
                "inner_status", "launch_id", "input_snapshot_id", "input_manifest_object_name",
                "input_manifest_generation", "input_manifest_sha256", "input_manifest_byte_count",
            }
            self.assertEqual(set(result), expected_keys)

    def test_twelve_paths_map_exactly_and_state_remains_outside_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "sub").mkdir(parents=True, exist_ok=True)
            (fx.roots["reference_root"] / "sub" / "a.txt").write_bytes(b"hello")
            (fx.roots["promoted_root"] / "b.json").write_bytes(b'{"x":1}')
            code, out, err, launch, parent, inner = fx.full_run()
            self.assertEqual(code, 0)
            self.assertEqual((parent / "reference_root" / "sub" / "a.txt").read_bytes(), b"hello")
            self.assertEqual((parent / "promoted_root" / "b.json").read_bytes(), b'{"x":1}')
            self.assertTrue((parent / "assembly-spec.json").exists())
            for root_name in ROOT_INPUT_NAMES:
                self.assertTrue((parent / root_name).is_dir())
            self.assertFalse((parent / "state").exists())

    def test_hydration_precedes_inner_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"hello")
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            cloud_control_id = _expected_hydration_control(launch, parent).control_id

            observations: dict[str, object] = {}

            def _observing_main(argv, **kwargs):
                observations["assembly_exists"] = (parent / "assembly-spec.json").exists()
                observations["reference_file_exists"] = (parent / "reference_root" / "a.txt").exists()
                observations["runtime_control_exists"] = (parent / "runtime-control.json").exists()
                sys.stdout.write(_envelope_text(_valid_envelope_body(launch, cloud_control_id)))
                return 0

            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=_observing_main,
            )
            self.assertEqual(code, 0)
            self.assertTrue(observations["assembly_exists"])
            self.assertTrue(observations["reference_file_exists"])
            self.assertTrue(observations["runtime_control_exists"])

    def test_prior_state_restore_is_passed_through_to_control(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            prior = PromotedOperationalGCSRestoreRequest(
                bucket=fx.spec.binding_bucket,
                manifest_object_name=f"promoted-operational-state/v1/2026-07-16/{'b' * 64}/manifests/{'c' * 64}.json",
                generation=1,
                expected_sha256="d" * 64,
                expected_spec_id="b" * 64,
            )
            code, out, err, launch, parent, inner = fx.full_run(launch_kwargs={"prior_state_restore": prior})
            self.assertEqual(code, 0)
            control = inner.calls[0]["control"]
            self.assertIsNotNone(control.prior_state_restore)
            self.assertEqual(control.prior_state_restore.manifest_object_name, prior.manifest_object_name)

    def test_exactly_one_client_shared_between_acquisition_and_inner_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            client_calls: list[int] = []
            code, out, err, launch, parent, inner = fx.full_run(
                gcs_client_factory=lambda: client_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 0)
            self.assertEqual(client_calls, [1])
            observed_client = inner.calls[0]["kwargs"]["gcs_client_factory"]()
            self.assertIs(observed_client, fx.client)


class LaunchAndRuntimeValidationTests(unittest.TestCase):
    def test_missing_launch_file_fails_before_runtime_and_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            parent = fx.runtime_parent()
            client_calls: list[int] = []
            code, out, err = fx.run(
                ["--launch-file", str(fx.tmp_path / "missing.json")],
                runtime_parent=parent, gcs_client_factory=lambda: client_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(client_calls, [])

    def test_relative_and_traversing_launch_file_paths_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with self.subTest(case="relative"):
                code, out, err = fx.run(["--launch-file", "relative-launch.json"])
                self.assertEqual(code, 2)
            with self.subTest(case="traversing"):
                code, out, err = fx.run(["--launch-file", str(fx.tmp_path / ".." / "escape.json")])
                self.assertEqual(code, 2)

    def test_runtime_path_cannot_be_supplied_through_cli(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            code, out, err = fx.run(
                ["--launch-file", str(launch_file), "--runtime-parent", str(fx.tmp_path / "runtime")]
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_nonempty_runtime_parent_fails_before_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            (parent / "leftover.txt").write_bytes(b"x")
            client_calls: list[int] = []
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: client_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 2)
            self.assertEqual(client_calls, [])

    def test_missing_runtime_parent_fails_before_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent(create=False)
            client_calls: list[int] = []
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: client_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 2)
            self.assertEqual(client_calls, [])

    def test_runtime_parent_that_is_a_file_fails_before_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.tmp_path / "not-a-dir"
            parent.write_bytes(b"x")
            client_calls: list[int] = []
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: client_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 2)
            self.assertEqual(client_calls, [])

    def test_runtime_parent_replacement_between_checks_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()

            original_recheck = hcj_module._recheck_runtime_parent
            calls = {"count": 0}

            def _fail_first(path, expected_identity):
                calls["count"] += 1
                if calls["count"] == 1:
                    raise PromotedOperationalHydratedCloudJobError("simulated parent replacement")
                return original_recheck(path, expected_identity)

            with mock.patch.object(hcj_module, "_recheck_runtime_parent", side_effect=_fail_first):
                code, out, err = fx.run(
                    ["--launch-file", str(launch_file)],
                    runtime_parent=parent, gcs_client_factory=lambda: fx.client,
                )
            self.assertEqual(code, 2)
            self.assertFalse((parent / "assembly-spec.json").exists())


class AcquisitionAndHydrationFailureTests(unittest.TestCase):
    def test_acquisition_failure_prevents_any_inner_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            inner = _FakeCloudJobMain(stdout_text="")

            broken_client = _FakeGCSClient()  # empty store: manifest object does not exist
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: broken_client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(inner.calls, [])
            self.assertFalse((parent / "assembly-spec.json").exists())

    def test_hydration_failure_from_preexisting_destination_prevents_inner_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            (parent / "reference_root").mkdir()  # collides with a hydration destination

            inner = _FakeCloudJobMain(stdout_text="")
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(inner.calls, [])

    def test_malicious_acquired_snapshot_return_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            inner = _FakeCloudJobMain(stdout_text="")

            with mock.patch.object(
                hcj_module, "acquire_promoted_operational_input_snapshot", return_value="not-acquired",
            ):
                code, out, err = fx.run(
                    ["--launch-file", str(launch_file)],
                    runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
                )
            self.assertEqual(code, 2)
            self.assertEqual(inner.calls, [])

    def test_malicious_completed_restore_return_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            inner = _FakeCloudJobMain(stdout_text="")

            with mock.patch.object(
                hcj_module, "hydrate_promoted_operational_input_snapshot", return_value="not-a-restore",
            ):
                code, out, err = fx.run(
                    ["--launch-file", str(launch_file)],
                    runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
                )
            self.assertEqual(code, 2)
            self.assertEqual(inner.calls, [])

    def test_control_write_failure_prevents_inner_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            inner = _FakeCloudJobMain(stdout_text="")

            with mock.patch.object(
                hcj_module, "_write_runtime_control_file", side_effect=OSError("disk full"),
            ):
                code, out, err = fx.run(
                    ["--launch-file", str(launch_file)],
                    runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
                )
            self.assertEqual(code, 2)
            self.assertEqual(inner.calls, [])


class InnerEnvelopeAdversarialTests(unittest.TestCase):
    def _run_with_body(self, fx: _Fixture, body_overrides: dict[str, object]):
        launch_file, launch = fx.launch_file()
        parent = fx.runtime_parent()
        cloud_control_id = _expected_hydration_control(launch, parent).control_id
        body = _valid_envelope_body(launch, cloud_control_id)
        body.update(body_overrides)
        inner = _FakeCloudJobMain(stdout_text=_envelope_text(body))
        code, out, err = fx.run(
            ["--launch-file", str(launch_file)],
            runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
        )
        return code, out, err

    def test_nonzero_inner_exit_code_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            inner = _FakeCloudJobMain(exit_code=2, stdout_text="")
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_nonempty_inner_stderr_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            cloud_control_id = _expected_hydration_control(launch, parent).control_id
            inner = _FakeCloudJobMain(
                stdout_text=_envelope_text(_valid_envelope_body(launch, cloud_control_id)),
                stderr_text="unexpected diagnostic",
            )
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_extra_trailing_stdout_line_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            cloud_control_id = _expected_hydration_control(launch, parent).control_id
            inner = _FakeCloudJobMain(
                stdout_text=_envelope_text(_valid_envelope_body(launch, cloud_control_id)) + "extra\n",
            )
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_noncanonical_inner_json_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            cloud_control_id = _expected_hydration_control(launch, parent).control_id
            body = _valid_envelope_body(launch, cloud_control_id)
            pretty = json.dumps(body, sort_keys=True) + "\n"
            inner = _FakeCloudJobMain(stdout_text=pretty)
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)

    def test_extra_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_extra_key(fx)
            self.assertEqual(code, 2)

    def _run_with_extra_key(self, fx: _Fixture):
        launch_file, launch = fx.launch_file()
        parent = fx.runtime_parent()
        cloud_control_id = _expected_hydration_control(launch, parent).control_id
        body = _valid_envelope_body(launch, cloud_control_id)
        body["extra_field"] = "x"
        inner = _FakeCloudJobMain(stdout_text=_envelope_text(body))
        return fx.run(
            ["--launch-file", str(launch_file)],
            runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
        )

    def test_missing_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            cloud_control_id = _expected_hydration_control(launch, parent).control_id
            body = _valid_envelope_body(launch, cloud_control_id)
            del body["binding_id"]
            inner = _FakeCloudJobMain(
                stdout_text=(
                    json.dumps(body, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
                    + "\n"
                )
            )
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)

    def test_wrong_assembly_spec_id_lineage_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"assembly_spec_id": "9" * 64})
            self.assertEqual(code, 2)

    def test_wrong_operational_run_spec_id_lineage_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"operational_run_spec_id": "9" * 64})
            self.assertEqual(code, 2)

    def test_wrong_target_session_lineage_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"target_session": "2099-01-01"})
            self.assertEqual(code, 2)

    def test_wrong_cloud_control_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"cloud_control_id": "9" * 64})
            self.assertEqual(code, 2)

    def test_non_str_runtime_job_spec_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"runtime_job_spec_id": 12345})
            self.assertEqual(code, 2)

    def test_non_positive_binding_generation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"binding_generation": 0})
            self.assertEqual(code, 2)

    def test_bool_binding_generation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"binding_generation": True})
            self.assertEqual(code, 2)

    def test_paper_only_false_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"paper_only": False})
            self.assertEqual(code, 2)

    def test_notification_eligible_true_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"notification_eligible": True})
            self.assertEqual(code, 2)

    def test_execution_eligible_true_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"execution_eligible": True})
            self.assertEqual(code, 2)

    def test_wrong_status_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"status": "SOMETHING_ELSE"})
            self.assertEqual(code, 2)

    def test_malformed_state_manifest_sha256_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(fx, {"state_manifest_sha256": "not-a-hash"})
            self.assertEqual(code, 2)

    def test_invalid_terminal_status_value_never_reaches_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            secret = "SECRET-TERMINAL-STATUS-VALUE"
            code, out, err = self._run_with_body(fx, {"terminal_status": secret})
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertNotIn(secret, out)
            self.assertNotIn(secret, err)

    def test_invalid_action_value_never_reaches_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            secret = "SECRET-ACTION-VALUE"
            code, out, err = self._run_with_body(fx, {"action": secret})
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertNotIn(secret, out)
            self.assertNotIn(secret, err)

    def test_invalid_failure_code_value_never_reaches_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            secret = "SECRET-FAILURE-CODE"
            code, out, err = self._run_with_body(
                fx,
                {
                    "terminal_status": "FAILED", "action": "NO_TRADE",
                    "failure_codes": [secret],
                },
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertNotIn(secret, out)
            self.assertNotIn(secret, err)

    def test_duplicate_failure_codes_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(
                fx,
                {
                    "terminal_status": "FAILED", "action": "NO_TRADE",
                    "failure_codes": ["QUOTE_ACQUISITION_FAILED", "QUOTE_ACQUISITION_FAILED"],
                },
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_unsorted_failure_codes_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(
                fx,
                {
                    "terminal_status": "FAILED", "action": "NO_TRADE",
                    "failure_codes": ["QUOTE_GATE_FAILED", "ALLOCATION_FAILED"],
                },
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_overlong_failure_codes_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(
                fx,
                {
                    "terminal_status": "FAILED", "action": "NO_TRADE",
                    "failure_codes": ["QUOTE_ACQUISITION_FAILED"] * 13,
                },
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_complete_with_failure_codes_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(
                fx,
                {"terminal_status": "COMPLETE", "failure_codes": ["QUOTE_ACQUISITION_FAILED"]},
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_failed_without_failure_codes_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(
                fx, {"terminal_status": "FAILED", "action": "NO_TRADE", "failure_codes": []},
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_failed_with_paper_buy_action_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = self._run_with_body(
                fx,
                {
                    "terminal_status": "FAILED", "action": "PAPER_BUY",
                    "failure_codes": ["QUOTE_ACQUISITION_FAILED"],
                },
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_state_manifest_path_session_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            cloud_control_id = _expected_hydration_control(launch, parent).control_id
            body = _valid_envelope_body(launch, cloud_control_id)
            body["state_manifest_object_name"] = (
                f"promoted-operational-state/v1/2099-01-01/"
                f"{launch.expected_operational_run_spec_id}/manifests/{'2' * 64}.json"
            )
            inner = _FakeCloudJobMain(stdout_text=_envelope_text(body))
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_state_manifest_path_spec_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            cloud_control_id = _expected_hydration_control(launch, parent).control_id
            body = _valid_envelope_body(launch, cloud_control_id)
            body["state_manifest_object_name"] = (
                f"promoted-operational-state/v1/{launch.target_session.isoformat()}/"
                f"{'9' * 64}/manifests/{'2' * 64}.json"
            )
            inner = _FakeCloudJobMain(stdout_text=_envelope_text(body))
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_state_manifest_path_publication_id_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            cloud_control_id = _expected_hydration_control(launch, parent).control_id
            body = _valid_envelope_body(launch, cloud_control_id)
            body["state_manifest_object_name"] = (
                f"promoted-operational-state/v1/{launch.target_session.isoformat()}/"
                f"{launch.expected_operational_run_spec_id}/manifests/{'9' * 64}.json"
            )
            inner = _FakeCloudJobMain(stdout_text=_envelope_text(body))
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_oversized_inner_stdout_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()
            oversized_stdout = "{" + ("x" * (hcj_module._MAXIMUM_INNER_STDOUT_BYTES + 1)) + "}\n"
            inner = _FakeCloudJobMain(stdout_text=oversized_stdout)
            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=lambda: fx.client, cloud_job_main=inner,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")


class SanitizationTests(unittest.TestCase):
    SECRET_MARKER = "SUPER-SECRET-HYDRATED-CLOUD-JOB-MARKER-7q1w"

    def test_reader_exception_containing_secret_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            launch_file, launch = fx.launch_file()
            parent = fx.runtime_parent()

            def _raising_factory():
                raise RuntimeError(f"leaked token={self.SECRET_MARKER}")

            code, out, err = fx.run(
                ["--launch-file", str(launch_file)],
                runtime_parent=parent, gcs_client_factory=_raising_factory,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(
                json.loads(err),
                {"error_type": "PromotedOperationalHydratedCloudJobError", "status": "FAILED"},
            )
            self.assertNotIn(self.SECRET_MARKER, err)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_forbidden_capability(self) -> None:
        source = inspect.getsource(hcj_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "socket", "subprocess", "requests", "urllib", "httpx",
            "kiteconnect", "time", "threading", "asyncio", "glob", "shutil", "tempfile",
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
            "list_blobs(", "list_objects(", "place_order(", "modify_order(", "cancel_order(",
            "sleep(", "gcloud", "gsutil", "docker", "os.environ.get(", "getenv(",
        ):
            self.assertNotIn(token, lowered, msg=token)

        # The module docstring legitimately explains, in prose, what this
        # boundary deliberately does NOT do (Telegram delivery, interactive
        # login) -- a plain substring scan over that prose would false-
        # positive. Instead assert the forbidden symbols are never actually
        # defined or imported.
        for forbidden_symbol in (
            "TelegramBotConfig", "TelegramDeliveryRequest", "deliver_telegram_notification",
            "UrllibTelegramHTTPTransport", "LocalTelegramDeliveryReceiptStore", "KiteLoginCredentials",
            "KiteMarketDataAdapter",
        ):
            self.assertFalse(hasattr(hcj_module, forbidden_symbol))

    def test_module_does_not_import_a_concrete_gcs_sdk_client_at_top_level(self) -> None:
        source = inspect.getsource(hcj_module)
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.Import):
                self.assertFalse(any(alias.name.split(".")[0] == "google" for alias in node.names))
            elif isinstance(node, ast.ImportFrom) and node.module:
                self.assertNotEqual(node.module.split(".")[0], "google")

    def test_fixed_runtime_parent_constant_matches_specification(self) -> None:
        self.assertEqual(str(hcj_module._FIXED_RUNTIME_PARENT), str(Path("/tmp/india-swing")))


if __name__ == "__main__":
    unittest.main()
