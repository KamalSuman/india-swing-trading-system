from __future__ import annotations

import ast
import inspect
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_cloud_control_prepare_cli as prepare_module
from india_swing.promoted_operational_assembly import (
    assemble_promoted_operational_runtime_inputs,
    encode_promoted_operational_assembly_spec,
)
from india_swing.promoted_operational_cloud_control import (
    decode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_cloud_control_prepare_cli import main

from tests import test_promoted_operational_assembly as assembly_tests
from tests import test_promoted_operational_job as job_tests


class _Fixture:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        tmp_path.mkdir(parents=True, exist_ok=True)
        self.preparation, _run_spec, _portfolio, self.artifact, self.spec = job_tests._fixture()
        self.assembly_file = tmp_path / "assembly.json"
        self.assembly_file.write_bytes(encode_promoted_operational_assembly_spec(self.spec))
        self.roots = {
            option: tmp_path / option[2:]
            for option in prepare_module._ROOT_OPTIONS
        }
        for root in self.roots.values():
            root.mkdir()
        self.state_root = tmp_path / "state-never-inspected"
        self.output_dir = tmp_path / "output"
        self.output_dir.mkdir()
        self.output_file = self.output_dir / "cloud-control.json"
        self.preparations = assembly_tests._FakePreparationResolver(self.preparation)
        self.portfolios = assembly_tests._FakePortfolioArtifactResolver(self.artifact)
        self.expected = assemble_promoted_operational_runtime_inputs(
            spec=self.spec,
            preparation_resolver=assembly_tests._FakePreparationResolver(self.preparation),
            portfolio_artifact_resolver=assembly_tests._FakePortfolioArtifactResolver(self.artifact),
        )

    def argv(self, *, prior: dict[str, str] | None = None, output: Path | None = None) -> list[str]:
        values: dict[str, str] = {
            "--assembly-spec-file": str(self.assembly_file),
            **{option: str(path) for option, path in self.roots.items()},
            "--state-root": str(self.state_root),
            "--output-control-file": str(output or self.output_file),
        }
        if prior:
            values.update(prior)
        argv: list[str] = []
        for option, value in values.items():
            argv.extend([option, value])
        return argv

    def prior(self, **overrides: str) -> dict[str, str]:
        publication_id = "c" * 64
        values = {
            "--prior-state-manifest-object-name": (
                f"promoted-operational-state/v1/{self.spec.target_session.isoformat()}/"
                f"{self.expected.run_spec.spec_id}/manifests/{publication_id}.json"
            ),
            "--prior-state-manifest-generation": "7",
            "--prior-state-manifest-sha256": "d" * 64,
        }
        values.update(overrides)
        return values

    def run(self, argv: list[str] | None = None):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(
                prepare_module,
                "build_promoted_operational_preparation_store",
                return_value=(object(), self.preparations),
            ) as build,
            mock.patch.object(
                prepare_module,
                "LocalSwingPortfolioArtifactStore",
                return_value=self.portfolios,
            ) as portfolio_store,
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            code = main(self.argv() if argv is None else argv)
        return code, stdout.getvalue(), stderr.getvalue(), build, portfolio_store


class SuccessAndRestartTests(unittest.TestCase):
    def test_first_run_derives_ids_and_writes_exact_control(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err, build, portfolio_store = fx.run()
            self.assertEqual(code, 0, err)
            self.assertEqual(err, "")
            self.assertEqual(build.call_count, 1)
            self.assertEqual(portfolio_store.call_count, 1)
            self.assertEqual(fx.preparations.calls, [fx.spec.preparation_id])
            self.assertEqual(fx.portfolios.calls, [fx.spec.portfolio_artifact_id])
            control = decode_promoted_operational_cloud_control(fx.output_file.read_bytes())
            self.assertEqual(control.expected_assembly_spec_id, fx.spec.assembly_spec_id)
            self.assertEqual(
                control.expected_operational_run_spec_id,
                fx.expected.run_spec.spec_id,
            )
            self.assertEqual(control.target_session, fx.spec.target_session)
            self.assertEqual(control.state_bucket, fx.spec.binding_bucket)
            self.assertEqual(control.assembly_spec_file, fx.assembly_file)
            self.assertEqual(control.state_root, fx.state_root)
            self.assertIsNone(control.prior_state_restore)
            result = json.loads(out)
            self.assertEqual(result["status"], "PROMOTED_OPERATIONAL_CLOUD_CONTROL_READY")
            self.assertEqual(result["control_id"], control.control_id)
            self.assertEqual(result["operational_run_spec_id"], fx.expected.run_spec.spec_id)
            self.assertFalse(result["prior_state_restore_present"])
            self.assertTrue(result["paper_only"])
            self.assertFalse(result["notification_eligible"])
            self.assertFalse(result["execution_eligible"])
            self.assertNotIn(str(fx.tmp_path), out)

    def test_same_run_restart_pin_is_reconstructed_from_exact_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err, _build, _portfolio = fx.run(fx.argv(prior=fx.prior()))
            self.assertEqual(code, 0, err)
            control = decode_promoted_operational_cloud_control(fx.output_file.read_bytes())
            self.assertIsNotNone(control.prior_state_restore)
            restore = control.prior_state_restore
            assert restore is not None
            self.assertEqual(restore.generation, 7)
            self.assertEqual(restore.expected_sha256, "d" * 64)
            self.assertEqual(restore.expected_spec_id, fx.expected.run_spec.spec_id)
            self.assertTrue(json.loads(out)["prior_state_restore_present"])

    def test_identical_replay_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            first = fx.run()
            original = fx.output_file.read_bytes()
            second = fx.run()
            self.assertEqual(first[0], 0, first[2])
            self.assertEqual(second[0], 0, second[2])
            self.assertEqual(fx.output_file.read_bytes(), original)


class FailClosedTests(unittest.TestCase):
    def _assert_failed(self, result) -> None:
        code, out, err, _build, _portfolio = result
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertEqual(
            json.loads(err),
            {
                "error_type": "PromotedOperationalCloudControlPrepareError",
                "status": "FAILED",
            },
        )

    def test_partial_prior_coordinates_fail_before_store_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            prior = {"--prior-state-manifest-generation": "1"}
            result = fx.run(fx.argv(prior=prior))
            self._assert_failed(result)
            self.assertEqual(result[3].call_count, 0)
            self.assertFalse(fx.output_file.exists())

    def test_malformed_prior_generation_hash_and_foreign_spec_path_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            cases = (
                {"--prior-state-manifest-generation": "01"},
                {"--prior-state-manifest-generation": "0"},
                {"--prior-state-manifest-sha256": "not-a-hash"},
                {
                    "--prior-state-manifest-object-name": (
                        "promoted-operational-state/v1/2026-07-16/"
                        f"{'9' * 64}/manifests/{'8' * 64}.json"
                    )
                },
            )
            for index, override in enumerate(cases):
                with self.subTest(index=index):
                    case_root = base / str(index)
                    case_root.mkdir()
                    fx = _Fixture(case_root)
                    self._assert_failed(fx.run(fx.argv(prior=fx.prior(**override))))
                    self.assertFalse(fx.output_file.exists())

    def test_output_inside_an_input_root_fails_before_store_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            output = fx.roots["--reference-root"] / "control.json"
            result = fx.run(fx.argv(output=output))
            self._assert_failed(result)
            self.assertEqual(result[3].call_count, 0)
            self.assertFalse(output.exists())

    def test_state_root_is_never_statted_or_created(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            original = prepare_module.os.lstat
            observed: list[Path] = []

            def recording(path):
                observed.append(Path(path))
                return original(path)

            with mock.patch.object(prepare_module.os, "lstat", side_effect=recording):
                result = fx.run()
            self.assertEqual(result[0], 0, result[2])
            self.assertNotIn(fx.state_root, observed)
            self.assertFalse(fx.state_root.exists())

    def test_missing_read_only_root_fails_before_store_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.roots["--reference-root"].rmdir()
            result = fx.run()
            self._assert_failed(result)
            self.assertEqual(result[3].call_count, 0)

    def test_divergent_existing_output_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.output_file.write_bytes(b"existing-different-content")
            self._assert_failed(fx.run())
            self.assertEqual(fx.output_file.read_bytes(), b"existing-different-content")

    def test_parent_replacement_before_exclusive_create_fails_without_false_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            parent = root / "output"
            parent.mkdir()
            displaced = root / "displaced"
            target = parent / "control.json"
            original = prepare_module._verify_parent_identity
            calls = {"count": 0}

            def replace_then_verify(path, identity):
                calls["count"] += 1
                if calls["count"] == 1:
                    path.rename(displaced)
                    path.mkdir()
                return original(path, identity)

            with mock.patch.object(
                prepare_module,
                "_verify_parent_identity",
                side_effect=replace_then_verify,
            ):
                with self.assertRaises(
                    prepare_module.PromotedOperationalCloudControlPrepareError
                ):
                    prepare_module._publish_control_file(target, b"{}\n")
            self.assertFalse(target.exists())
            self.assertFalse((displaced / "control.json").exists())

    def test_parent_replacement_after_existing_replay_read_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            parent = root / "output"
            parent.mkdir()
            displaced = root / "displaced"
            target = parent / "control.json"
            payload = b"{}\n"
            target.write_bytes(payload)
            original_read = prepare_module.read_stable_regular_file

            def read_then_replace(path, *, maximum_bytes):
                value = original_read(path, maximum_bytes=maximum_bytes)
                parent.rename(displaced)
                parent.mkdir()
                return value

            with mock.patch.object(
                prepare_module,
                "read_stable_regular_file",
                side_effect=read_then_replace,
            ):
                with self.assertRaises(
                    prepare_module.PromotedOperationalCloudControlPrepareError
                ):
                    prepare_module._publish_control_file(target, payload)
            self.assertFalse(target.exists())
            self.assertEqual((displaced / "control.json").read_bytes(), payload)

    def test_source_assembly_mutation_during_preflight_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            changed = replace(fx.spec, binding_bucket="different-bucket")
            with mock.patch.object(
                prepare_module,
                "load_promoted_operational_assembly_spec_file",
                side_effect=(fx.spec, changed),
            ):
                self._assert_failed(fx.run())
            self.assertFalse(fx.output_file.exists())

    def test_parser_rejects_missing_duplicate_unknown_relative_and_traversing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            for index, argv in enumerate(
                (
                    [],
                    ["--unknown", str(root / "x")],
                    ["--assembly-spec-file", "relative.json"],
                    ["--assembly-spec-file", str(root / ".." / "escape.json")],
                )
            ):
                with self.subTest(index=index):
                    stdout = io.StringIO()
                    stderr = io.StringIO()
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        code = main(argv)
                    self.assertEqual(code, 2)
                    self.assertEqual(stdout.getvalue(), "")

            fx = _Fixture(root / "fixture")
            duplicated = fx.argv() + ["--state-root", str(fx.state_root)]
            self._assert_failed(fx.run(duplicated))

    def test_store_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            secret = "SUPER-SECRET-CONTROL-PREPARE-MARKER"
            with mock.patch.object(
                prepare_module,
                "build_promoted_operational_preparation_store",
                side_effect=RuntimeError(secret),
            ):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = main(fx.argv())
            self.assertEqual(code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertNotIn(secret, stderr.getvalue())


class RegistrationAndCapabilityTests(unittest.TestCase):
    def test_pyproject_console_script_mapping_is_exact(self) -> None:
        pyproject = Path(inspect.getfile(prepare_module)).resolve().parent.parent.parent / "pyproject.toml"
        self.assertIn(
            'india-swing-promoted-operational-cloud-control-prepare = '
            '"india_swing.promoted_operational_cloud_control_prepare_cli:main"',
            pyproject.read_text(encoding="utf-8"),
        )

    def test_module_has_no_online_or_execution_capability(self) -> None:
        source = inspect.getsource(prepare_module)
        tree = ast.parse(source)
        forbidden_modules = {
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
            "telegram",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden_modules, set())
        for token in (
            "os.environ",
            "getenv(",
            "datetime.now(",
            "list_blobs(",
            "storage.client(",
            "place_order(",
            "sleep(",
            "gcloud",
        ):
            self.assertNotIn(token, source.lower())


if __name__ == "__main__":
    unittest.main()
