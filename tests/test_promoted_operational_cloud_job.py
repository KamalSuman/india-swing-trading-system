from __future__ import annotations

import ast
import inspect
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_cloud_job as cj_module
from india_swing.market_data.kite import KiteMarketDataAdapter
from india_swing.operations.portfolio_store import LocalSwingPortfolioArtifactStore
from india_swing.promoted_operational_anchored_session import (
    run_publish_and_anchor_promoted_operational_session,
)
from india_swing.promoted_operational_assembly import (
    assemble_promoted_operational_runtime_inputs,
    encode_promoted_operational_assembly_spec,
)
from india_swing.promoted_operational_cloud_control import (
    PromotedOperationalCloudRunControl,
    encode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_cloud_job import PromotedOperationalCloudJobError, main
from india_swing.daily_pipeline.acquisition import GoogleCloudStorageObjectReader
from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_gcs_state import (
    CompletedPromotedOperationalGCSPublication,
    PromotedOperationalGCSRestoreRequest,
    publish_promoted_operational_state_to_gcs,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
)
from india_swing.promoted_operational_runtime import PromotedOperationalRuntimeState

from tests import test_promoted_operational_anchored_session as _anchored_tests
from tests import test_promoted_operational_job as _job_tests
from tests import test_promoted_operational_runner as _runner_tests


class _FakeBlob:
    def __init__(self, store, bucket_name, object_name, requested_generation):
        self._store = store
        self._key = (bucket_name, object_name)
        self.name = object_name
        self.requested_generation = requested_generation
        self.generation = requested_generation

    def upload_from_string(self, content_bytes, *, content_type, if_generation_match, checksum, retry):
        # google-api-core is not installed in this test environment, so
        # GoogleCloudStorageStateObjectWriter's own PreconditionFailed-based
        # conflict-then-reload-then-verify path (already tested elsewhere)
        # cannot be exercised here. A same-content re-upload at an existing
        # path is instead treated as an idempotent no-op success (matching
        # what create_or_verify's caller actually observes); differing
        # content raises a generic failure.
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
            from google.api_core.exceptions import PreconditionFailed

            raise PreconditionFailed("generation mismatch")
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


class _FakeInnerMain:
    """Records every call and lets a test dictate the inner job's exact
    exit code / stdout / stderr without running the real assembly/Kite/
    quote-gate pipeline -- used only for adversarial-table tests of the
    wrapper's own post-inner-job validation."""

    def __init__(self, *, exit_code: int = 0, stdout_text: str = "", stderr_text: str = "") -> None:
        self.exit_code = exit_code
        self.stdout_text = stdout_text
        self.stderr_text = stderr_text
        self.calls: list[dict[str, object]] = []

    def __call__(self, argv, **kwargs) -> int:
        self.calls.append({"argv": list(argv), "kwargs": dict(kwargs)})
        sys.stdout.write(self.stdout_text)
        sys.stderr.write(self.stderr_text)
        return self.exit_code


class _Fixture:
    def __init__(self, tmp_path: Path, *, open_positions: int = 0) -> None:
        self.tmp_path = tmp_path
        self.preparation, _run_spec, self.portfolio, self.artifact, self.spec = _job_tests._fixture(
            open_positions=open_positions
        )
        self.portfolio_artifact_root = tmp_path / "portfolio-artifact"
        LocalSwingPortfolioArtifactStore(self.portfolio_artifact_root).put(self.artifact)
        self.assembly_spec_file = tmp_path / "assembly.json"
        self.assembly_spec_file.write_bytes(encode_promoted_operational_assembly_spec(self.spec))
        self.assembly = assemble_promoted_operational_runtime_inputs(
            spec=self.spec,
            preparation_resolver=_job_tests._FakePreparationResolver(self.preparation),
            portfolio_artifact_resolver=LocalSwingPortfolioArtifactStore(self.portfolio_artifact_root),
        )
        self.state_root = tmp_path / "state"
        self.client = _FakeGCSClient()
        self.backend = _anchored_tests._FakeBindingBackend()
        self.preflight = _anchored_tests._PermissiveRecordingPreflight()

    def roots_kwargs(self, *, state_root: Path | None = None) -> dict[str, Path]:
        return {
            "reference_root": self.tmp_path / "reference-root",
            "identity_evidence_root": self.tmp_path / "identity-evidence-root",
            "calendar_root": self.tmp_path / "calendar-root",
            "daily_reports_root": self.tmp_path / "daily-reports-root",
            "historical_corpus_root": self.tmp_path / "historical-corpus-root",
            "promoted_root": self.tmp_path / "promoted-root",
            "graph_publication_root": self.tmp_path / "graph-publication-root",
            "engine_run_root": self.tmp_path / "engine-run-root",
            "research_run_root": self.tmp_path / "research-run-root",
            "operational_preparation_root": self.tmp_path / "operational-preparation-root",
            "portfolio_artifact_root": self.portfolio_artifact_root,
            "state_root": state_root if state_root is not None else self.state_root,
        }

    def control(
        self, *, prior_state_restore=None, state_root: Path | None = None
    ) -> PromotedOperationalCloudRunControl:
        return PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=self.spec.assembly_spec_id,
            expected_operational_run_spec_id=self.assembly.run_spec.spec_id,
            target_session=self.spec.target_session,
            state_bucket=self.spec.binding_bucket,
            assembly_spec_file=self.assembly_spec_file,
            prior_state_restore=prior_state_restore,
            **self.roots_kwargs(state_root=state_root),
        )

    def control_file(self, control: PromotedOperationalCloudRunControl | None = None, *, name: str = "control.json") -> Path:
        if control is None:
            control = self.control()
        path = self.tmp_path / name
        path.write_bytes(encode_promoted_operational_cloud_control(control))
        return path

    def runtime_callable(self, *, quote_source=None):
        if quote_source is None:
            quote_source = _runner_tests._FakeQuoteSource(
                responder=_runner_tests._sorted_quote_responder(self.preparation),
                source_id=self.spec.expected_quote_source_id,
            )

        def _fake(**kwargs):
            anchored = run_publish_and_anchor_promoted_operational_session(
                spec=kwargs["run_spec"],
                quote_source=quote_source,
                portfolio_source=kwargs["portfolio_source"],
                clock=kwargs["clock"],
                advisory_outbox=kwargs["advisory_outbox"],
                terminal_store=kwargs["terminal_store"],
                paper_ledger=kwargs["paper_ledger"],
                binding_bucket=kwargs["job_spec"].binding_bucket,
                binding_writer=self.backend,
                binding_reader=self.backend,
                binding_preflight=self.preflight,
            )
            return PromotedOperationalRuntimeState(job_spec=kwargs["job_spec"], anchored=anchored)

        return _fake

    def run(self, argv, **kwargs):
        common: dict[str, object] = dict(
            environ={"INDIA_SWING_KITE_API_KEY": "k", "INDIA_SWING_KITE_ACCESS_TOKEN": "t"},
            kite_adapter_factory=lambda creds, clk: KiteMarketDataAdapter(
                object(), sdk_version="test-fixture/1.0", clock=clk
            ),
            gcs_client_factory=lambda: self.client,
            runtime_callable=self.runtime_callable(),
        )
        common.update(kwargs)
        with mock.patch.object(
            _job_tests._job_module,
            "build_promoted_operational_preparation_store",
            return_value=(None, _job_tests._FakePreparationResolver(self.preparation)),
        ):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(argv, **common)
            return code, stdout.getvalue(), stderr.getvalue()

    def terminal(self):
        return LocalPromotedOperationalTerminalStore(self.state_root / "terminal").get(
            self.assembly.run_spec.spec_id
        )


def _inner_envelope_for(fx: _Fixture, terminal, **overrides) -> dict[str, object]:
    body = {
        "status": "PROMOTED_OPERATIONAL_JOB_COMPLETE",
        "assembly_spec_id": fx.spec.assembly_spec_id,
        "runtime_job_spec_id": fx.assembly.runtime_job_spec.job_spec_id,
        "operational_run_spec_id": terminal.spec_id,
        "preparation_id": terminal.preparation_id,
        "target_session": terminal.target_session.isoformat(),
        "terminal_id": terminal.terminal_id,
        "terminal_status": terminal.status.value,
        "action": terminal.action.value,
        "failure_codes": list(terminal.failure_codes),
        "advisory_id": terminal.advisory_id,
        "binding_id": "f" * 64,
        "binding_generation": 1,
        "reused_existing_terminal": False,
        "paper_only": True,
        "notification_eligible": False,
        "execution_eligible": False,
    }
    body.update(overrides)
    return body


def _inner_envelope_text(body: dict[str, object]) -> str:
    return json.dumps(body, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"


class FileBoundaryTests(unittest.TestCase):
    def test_missing_control_file_fails_before_gcs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            gcs_calls: list[int] = []
            code, out, err = fx.run(
                ["--control-file", str(fx.tmp_path / "missing.json")],
                gcs_client_factory=lambda: gcs_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(json.loads(err), {"error_type": "PromotedOperationalCloudJobError", "status": "FAILED"})
            self.assertEqual(gcs_calls, [])

    def test_relative_control_file_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = fx.run(["--control-file", "relative-control.json"])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_traversing_control_file_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            code, out, err = fx.run(
                ["--control-file", str(fx.tmp_path / ".." / "escape.json")]
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_directory_control_file_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            directory_path = fx.tmp_path / "a-directory"
            directory_path.mkdir()
            code, out, err = fx.run(["--control-file", str(directory_path)])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_oversized_control_file_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            oversized = fx.tmp_path / "oversized.json"
            oversized.write_bytes(b"{}" + b" " * (200 * 1024))
            code, out, err = fx.run(["--control-file", str(oversized)])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_unknown_and_duplicate_cli_arguments_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()

            with self.subTest(case="unknown"):
                code, out, err = fx.run(["--control-file", str(control_file), "--extra", "x"])
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

            with self.subTest(case="duplicate"):
                code, out, err = fx.run(
                    ["--control-file", str(control_file), "--control-file", str(control_file)]
                )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

            with self.subTest(case="missing_value"):
                code, out, err = fx.run(["--control-file"])
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

    def test_assembly_identity_mismatch_fails_before_gcs_and_kite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            wrong_control = PromotedOperationalCloudRunControl(
                expected_assembly_spec_id="9" * 64,
                expected_operational_run_spec_id=fx.assembly.run_spec.spec_id,
                target_session=fx.spec.target_session,
                state_bucket=fx.spec.binding_bucket,
                assembly_spec_file=fx.assembly_spec_file,
                **fx.roots_kwargs(),
            )
            control_file = fx.control_file(wrong_control)
            gcs_calls: list[int] = []
            kite_calls: list[int] = []
            code, out, err = fx.run(
                ["--control-file", str(control_file)],
                gcs_client_factory=lambda: gcs_calls.append(1) or fx.client,
                kite_adapter_factory=lambda creds, clk: kite_calls.append(1) or KiteMarketDataAdapter(
                    object(), sdk_version="test-fixture/1.0", clock=clk
                ),
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(gcs_calls, [])
            self.assertEqual(kite_calls, [])


class FreshPathTests(unittest.TestCase):
    def test_one_client_zero_restore_one_inner_call_and_exact_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            gcs_calls: list[int] = []

            def _factory():
                gcs_calls.append(1)
                return fx.client

            code, out, err = fx.run(["--control-file", str(control_file)], gcs_client_factory=_factory)
            self.assertEqual(err, "")
            self.assertEqual(code, 0)
            self.assertEqual(len(gcs_calls), 1)

            envelope = json.loads(out)
            self.assertEqual(
                set(envelope),
                {
                    "status", "assembly_spec_id", "runtime_job_spec_id", "operational_run_spec_id",
                    "preparation_id", "target_session", "terminal_id", "terminal_status", "action",
                    "failure_codes", "advisory_id", "binding_id", "binding_generation",
                    "reused_existing_terminal", "paper_only", "notification_eligible",
                    "execution_eligible", "cloud_control_id", "state_publication_id",
                    "state_manifest_object_name", "state_manifest_generation",
                    "state_manifest_sha256", "state_manifest_byte_count",
                },
            )
            self.assertEqual(envelope["status"], "PROMOTED_OPERATIONAL_JOB_COMPLETE")
            self.assertEqual(envelope["operational_run_spec_id"], fx.assembly.run_spec.spec_id)
            self.assertIs(envelope["paper_only"], True)
            self.assertIs(envelope["notification_eligible"], False)
            self.assertIs(envelope["execution_eligible"], False)
            self.assertGreater(envelope["state_manifest_generation"], 0)
            self.assertEqual(len(envelope["state_manifest_sha256"]), 64)

            terminal = fx.terminal()
            self.assertEqual(envelope["terminal_id"], terminal.terminal_id)
            self.assertEqual(envelope["cloud_control_id"], fx.control().control_id)


class RestoreReplayPathTests(unittest.TestCase):
    def test_restore_precedes_inner_job_and_republish_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            code1, out1, err1 = fx.run(["--control-file", str(control_file)])
            self.assertEqual(code1, 0, err1)
            envelope1 = json.loads(out1)

            restore = PromotedOperationalGCSRestoreRequest(
                bucket=fx.spec.binding_bucket,
                manifest_object_name=envelope1["state_manifest_object_name"],
                generation=envelope1["state_manifest_generation"],
                expected_sha256=envelope1["state_manifest_sha256"],
                expected_spec_id=fx.assembly.run_spec.spec_id,
            )

            fresh_state_root = fx.tmp_path / "state-restart"
            restart_control = fx.control(prior_state_restore=restore, state_root=fresh_state_root)
            restart_control_file = fx.control_file(restart_control, name="restart-control.json")

            def _exploding_quote_responder(_keys=None):
                raise AssertionError("quote acquisition must not happen on replay")

            exploding_quote_source = _runner_tests._FakeQuoteSource(
                responder=_exploding_quote_responder, source_id=fx.spec.expected_quote_source_id
            )
            code2, out2, err2 = fx.run(
                ["--control-file", str(restart_control_file)],
                runtime_callable=fx.runtime_callable(quote_source=exploding_quote_source),
            )
            self.assertEqual(code2, 0, err2)
            envelope2 = json.loads(out2)
            self.assertEqual(envelope2["terminal_id"], envelope1["terminal_id"])
            self.assertEqual(envelope2["state_publication_id"], envelope1["state_publication_id"])
            self.assertTrue(envelope2["reused_existing_terminal"])

    def test_restore_failure_causes_zero_inner_and_publication_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            bogus_restore = PromotedOperationalGCSRestoreRequest(
                bucket=fx.spec.binding_bucket,
                manifest_object_name=(
                    f"promoted-operational-state/v1/{fx.spec.target_session.isoformat()}/"
                    f"{fx.assembly.run_spec.spec_id}/manifests/{'9' * 64}.json"
                ),
                generation=1,
                expected_sha256="9" * 64,
                expected_spec_id=fx.assembly.run_spec.spec_id,
            )
            control = fx.control(prior_state_restore=bogus_restore)
            control_file = fx.control_file(control)
            inner_main = _FakeInnerMain()
            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(inner_main.calls, [])


class InnerJobAdversarialTests(unittest.TestCase):
    def _real_terminal(self, fx: _Fixture):
        control_file = fx.control_file()
        code, out, err = fx.run(["--control-file", str(control_file)])
        self.assertEqual(code, 0, err)
        return fx.terminal(), control_file

    def test_nonzero_exit_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            terminal, control_file = self._real_terminal(fx)
            envelope = _inner_envelope_for(fx, terminal)
            inner_main = _FakeInnerMain(exit_code=2, stdout_text=_inner_envelope_text(envelope))
            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(len(inner_main.calls), 1)

    def test_stderr_content_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            terminal, control_file = self._real_terminal(fx)
            envelope = _inner_envelope_for(fx, terminal)
            inner_main = _FakeInnerMain(
                stdout_text=_inner_envelope_text(envelope), stderr_text="warning: something\n"
            )
            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_empty_stdout_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            _terminal, control_file = self._real_terminal(fx)
            inner_main = _FakeInnerMain(stdout_text="")
            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_multiple_lines_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            terminal, control_file = self._real_terminal(fx)
            envelope = _inner_envelope_for(fx, terminal)
            inner_main = _FakeInnerMain(
                stdout_text=_inner_envelope_text(envelope) + _inner_envelope_text(envelope)
            )
            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_malformed_json_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            _terminal, control_file = self._real_terminal(fx)
            inner_main = _FakeInnerMain(stdout_text="{not-json\n")
            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_noncanonical_json_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            terminal, control_file = self._real_terminal(fx)
            envelope = _inner_envelope_for(fx, terminal)
            pretty = json.dumps(envelope, sort_keys=True, indent=2) + "\n"
            inner_main = _FakeInnerMain(stdout_text=pretty)
            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_unknown_and_missing_keys_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            terminal, control_file = self._real_terminal(fx)

            with self.subTest(case="unknown_key"):
                envelope = _inner_envelope_for(fx, terminal)
                envelope["extra"] = "x"
                inner_main = _FakeInnerMain(stdout_text=_inner_envelope_text(envelope))
                code, out, err = fx.run(
                    ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
                )
                self.assertEqual(code, 2)

            with self.subTest(case="missing_key"):
                envelope = _inner_envelope_for(fx, terminal)
                del envelope["binding_id"]
                inner_main = _FakeInnerMain(stdout_text=_inner_envelope_text(envelope))
                code, out, err = fx.run(
                    ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
                )
                self.assertEqual(code, 2)

    def test_wrong_status_assembly_spec_session_or_authority_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            terminal, control_file = self._real_terminal(fx)

            for field, value in (
                ("status", "SOMETHING_ELSE"),
                ("assembly_spec_id", "9" * 64),
                ("operational_run_spec_id", "9" * 64),
                ("target_session", "2000-01-01"),
                ("paper_only", False),
                ("notification_eligible", True),
                ("execution_eligible", True),
            ):
                with self.subTest(field=field):
                    envelope = _inner_envelope_for(fx, terminal, **{field: value})
                    inner_main = _FakeInnerMain(stdout_text=_inner_envelope_text(envelope))
                    code, out, err = fx.run(
                        ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
                    )
                    self.assertEqual(code, 2)
                    self.assertEqual(out, "")


class PostRunAdversarialTests(unittest.TestCase):
    def _real_terminal(self, fx: _Fixture):
        control_file = fx.control_file()
        code, out, err = fx.run(["--control-file", str(control_file)])
        self.assertEqual(code, 0, err)
        return fx.terminal(), control_file

    def test_missing_terminal_in_exact_store_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            # Never run the real flow -- the wrapper's own terminal store is
            # empty, so a lookup by the control's exact expected spec ID
            # must fail even though the (faked) inner job reports success.
            control_file = fx.control_file()
            fake_terminal_spec_id = fx.assembly.run_spec.spec_id
            envelope = {
                "status": "PROMOTED_OPERATIONAL_JOB_COMPLETE",
                "assembly_spec_id": fx.spec.assembly_spec_id,
                "runtime_job_spec_id": "a" * 64,
                "operational_run_spec_id": fake_terminal_spec_id,
                "preparation_id": "b" * 64,
                "target_session": fx.spec.target_session.isoformat(),
                "terminal_id": "c" * 64,
                "terminal_status": "FAILED",
                "action": "NO_TRADE",
                "failure_codes": [],
                "advisory_id": "d" * 64,
                "binding_id": "e" * 64,
                "binding_generation": 1,
                "reused_existing_terminal": False,
                "paper_only": True,
                "notification_eligible": False,
                "execution_eligible": False,
            }
            inner_main = _FakeInnerMain(stdout_text=_inner_envelope_text(envelope))
            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_mismatched_inner_envelope_against_real_terminal_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            terminal, control_file = self._real_terminal(fx)

            for field in ("terminal_id", "advisory_id", "action", "terminal_status", "preparation_id"):
                with self.subTest(field=field):
                    envelope = _inner_envelope_for(fx, terminal, **{field: "9" * 64 if field != "action" and field != "terminal_status" else "NO_TRADE"})
                    if field == "action":
                        envelope["action"] = "PAPER_BUY" if terminal.action.value != "PAPER_BUY" else "NO_TRADE"
                    if field == "terminal_status":
                        envelope["terminal_status"] = "COMPLETE" if terminal.status.value != "COMPLETE" else "FAILED"
                    inner_main = _FakeInnerMain(stdout_text=_inner_envelope_text(envelope))
                    code, out, err = fx.run(
                        ["--control-file", str(control_file)], promoted_operational_job_main=inner_main
                    )
                    self.assertEqual(code, 2)
                    self.assertEqual(out, "")

    def test_publication_writer_exception_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()

            class _ExplodingBucket:
                def blob(self, object_name, generation=None):
                    raise RuntimeError("boom")

            class _ExplodingClient:
                def bucket(self, name):
                    return _ExplodingBucket()

            code, out, err = fx.run(
                ["--control-file", str(control_file)], gcs_client_factory=lambda: _ExplodingClient()
            )
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_malicious_completed_publication_return_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            with mock.patch.object(
                cj_module, "publish_promoted_operational_state_to_gcs", return_value=object()
            ):
                code, out, err = fx.run(["--control-file", str(control_file)])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")


class CallOrderTests(unittest.TestCase):
    def test_control_and_assembly_validation_precede_client_construction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            bad_control_file = fx.tmp_path / "bad-control.json"
            bad_control_file.write_bytes(b"not-a-valid-control")
            gcs_calls: list[int] = []
            code, out, err = fx.run(
                ["--control-file", str(bad_control_file)],
                gcs_client_factory=lambda: gcs_calls.append(1) or fx.client,
            )
            self.assertEqual(code, 2)
            self.assertEqual(gcs_calls, [])

    def test_exactly_one_client_and_one_inner_call_on_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            gcs_calls: list[int] = []
            inner_calls: list[int] = []
            original_inner = _job_tests._job_module.main

            def _tracked_inner(argv, **kwargs):
                inner_calls.append(1)
                return original_inner(argv, **kwargs)

            code, out, err = fx.run(
                ["--control-file", str(control_file)],
                gcs_client_factory=lambda: gcs_calls.append(1) or fx.client,
                promoted_operational_job_main=_tracked_inner,
            )
            self.assertEqual(code, 0, err)
            self.assertEqual(len(gcs_calls), 1)
            self.assertEqual(len(inner_calls), 1)


class SanitizationTests(unittest.TestCase):
    SECRET_MARKER = "SUPER-SECRET-CLOUD-JOB-MARKER-7q2z"

    def _assert_sanitized(self, code: int, out: str, err: str) -> None:
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertEqual(
            json.loads(err), {"error_type": "PromotedOperationalCloudJobError", "status": "FAILED"}
        )
        self.assertNotIn(self.SECRET_MARKER, err)

    def test_file_reader_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                cj_module, "read_stable_regular_file",
                side_effect=RuntimeError(f"leak {self.SECRET_MARKER}"),
            ):
                code, out, err = fx.run(["--control-file", str(fx.control_file())])
            self._assert_sanitized(code, out, err)

    def test_gcs_client_factory_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())

            def _exploding_factory():
                raise RuntimeError(f"leak {self.SECRET_MARKER}")

            code, out, err = fx.run(
                ["--control-file", str(fx.control_file())], gcs_client_factory=_exploding_factory
            )
            self._assert_sanitized(code, out, err)

    def test_restore_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            bogus_restore = PromotedOperationalGCSRestoreRequest(
                bucket=fx.spec.binding_bucket,
                manifest_object_name=(
                    f"promoted-operational-state/v1/{fx.spec.target_session.isoformat()}/"
                    f"{fx.assembly.run_spec.spec_id}/manifests/{'9' * 64}.json"
                ),
                generation=1,
                expected_sha256="9" * 64,
                expected_spec_id=fx.assembly.run_spec.spec_id,
            )
            control = fx.control(prior_state_restore=bogus_restore)
            control_file = fx.control_file(control)

            class _RaisingReader:
                def read_generation(self, **kwargs):
                    raise RuntimeError(f"leak {self.outer_marker}")

            reader = _RaisingReader()
            reader.outer_marker = self.SECRET_MARKER
            with mock.patch.object(cj_module, "GoogleCloudStorageObjectReader", return_value=reader):
                code, out, err = fx.run(["--control-file", str(control_file)])
            self._assert_sanitized(code, out, err)

    def test_inner_main_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()

            def _exploding_inner(argv, **kwargs):
                raise RuntimeError(f"leak {self.SECRET_MARKER}")

            code, out, err = fx.run(
                ["--control-file", str(control_file)], promoted_operational_job_main=_exploding_inner
            )
            self._assert_sanitized(code, out, err)

    def test_local_store_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            with mock.patch.object(
                cj_module.LocalPromotedOperationalTerminalStore,
                "get",
                side_effect=RuntimeError(f"leak {self.SECRET_MARKER}"),
            ):
                code, out, err = fx.run(["--control-file", str(control_file)])
            self._assert_sanitized(code, out, err)

    def test_publisher_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            with mock.patch.object(
                cj_module, "publish_promoted_operational_state_to_gcs",
                side_effect=RuntimeError(f"leak {self.SECRET_MARKER}"),
            ):
                code, out, err = fx.run(["--control-file", str(control_file)])
            self._assert_sanitized(code, out, err)


class Revision2InnerEnvelopeFieldValidationTests(unittest.TestCase):
    SECRET_MARKER = "SUPER-SECRET-ENVELOPE-FIELD-MARKER-3x9k"

    def _real_terminal(self, fx: _Fixture):
        control_file = fx.control_file()
        code, out, err = fx.run(["--control-file", str(control_file)])
        self.assertEqual(code, 0, err)
        return fx.terminal(), control_file

    def _assert_blocked_and_not_echoed(self, code: int, out: str, err: str) -> None:
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertNotIn(self.SECRET_MARKER, err)
        self.assertEqual(
            json.loads(err), {"error_type": "PromotedOperationalCloudJobError", "status": "FAILED"}
        )

    def _run_with_override(self, fx, control_file, **overrides):
        terminal = fx.terminal()
        envelope = _inner_envelope_for(fx, terminal, **overrides)
        inner_main = _FakeInnerMain(stdout_text=_inner_envelope_text(envelope))
        return fx.run(["--control-file", str(control_file)], promoted_operational_job_main=inner_main)

    def test_secret_non_sha_runtime_job_spec_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            _terminal, control_file = self._real_terminal(fx)
            code, out, err = self._run_with_override(
                fx, control_file, runtime_job_spec_id=self.SECRET_MARKER
            )
            self._assert_blocked_and_not_echoed(code, out, err)

    def test_secret_non_sha_binding_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            _terminal, control_file = self._real_terminal(fx)
            code, out, err = self._run_with_override(fx, control_file, binding_id=self.SECRET_MARKER)
            self._assert_blocked_and_not_echoed(code, out, err)

    def test_bool_binding_generation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            _terminal, control_file = self._real_terminal(fx)
            code, out, err = self._run_with_override(fx, control_file, binding_generation=True)
            self._assert_blocked_and_not_echoed(code, out, err)

    def test_zero_binding_generation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            _terminal, control_file = self._real_terminal(fx)
            code, out, err = self._run_with_override(fx, control_file, binding_generation=0)
            self._assert_blocked_and_not_echoed(code, out, err)

    def test_negative_binding_generation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            _terminal, control_file = self._real_terminal(fx)
            code, out, err = self._run_with_override(fx, control_file, binding_generation=-1)
            self._assert_blocked_and_not_echoed(code, out, err)

    def test_non_bool_reused_existing_terminal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            _terminal, control_file = self._real_terminal(fx)
            code, out, err = self._run_with_override(fx, control_file, reused_existing_terminal=1)
            self._assert_blocked_and_not_echoed(code, out, err)


class Revision2CompletedPublicationReconstructionTests(unittest.TestCase):
    def _real_publication(self, fx: _Fixture):
        control_file = fx.control_file()
        code, out, err = fx.run(["--control-file", str(control_file)])
        self.assertEqual(code, 0, err)
        terminal = fx.terminal()
        writer = GoogleCloudStorageStateObjectWriter(client=fx.client)
        advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(fx.state_root / "advisory")
        paper_ledger = LocalPaperTradeLedger(fx.state_root / "paper")
        publication = publish_promoted_operational_state_to_gcs(
            terminal=terminal,
            bucket=fx.spec.binding_bucket,
            writer=writer,
            advisory_outbox=advisory_outbox,
            paper_ledger=paper_ledger,
        )
        return control_file, terminal, publication

    def test_wrong_type_publication_return_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file = fx.control_file()
            with mock.patch.object(
                cj_module, "publish_promoted_operational_state_to_gcs", return_value="not-a-publication"
            ):
                code, out, err = fx.run(["--control-file", str(control_file)])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_post_construction_manifest_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file, terminal, publication = self._real_publication(fx)
            object.__setattr__(publication.manifest, "preparation_id", "9" * 64)
            with mock.patch.object(
                cj_module, "publish_promoted_operational_state_to_gcs", return_value=publication
            ):
                code, out, err = fx.run(["--control-file", str(control_file)])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_post_construction_manifest_object_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file, terminal, publication = self._real_publication(fx)
            object.__setattr__(
                publication.manifest_object, "byte_count", publication.manifest_object.byte_count + 1
            )
            with mock.patch.object(
                cj_module, "publish_promoted_operational_state_to_gcs", return_value=publication
            ):
                code, out, err = fx.run(["--control-file", str(control_file)])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_bool_generation_in_manifest_object_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            control_file, terminal, publication = self._real_publication(fx)
            object.__setattr__(publication.manifest_object, "generation", True)
            with mock.patch.object(
                cj_module, "publish_promoted_operational_state_to_gcs", return_value=publication
            ):
                code, out, err = fx.run(["--control-file", str(control_file)])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_self_consistent_foreign_manifest_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as outer_directory:
            outer = Path(outer_directory).resolve()
            (outer / "a").mkdir(parents=True, exist_ok=True)
            (outer / "b").mkdir(parents=True, exist_ok=True)
            fx_a = _Fixture(outer / "a")
            fx_b = _Fixture(outer / "b")
            control_file_a, terminal_a, _publication_a = self._real_publication(fx_a)
            _control_file_b, _terminal_b, publication_b = self._real_publication(fx_b)

            with mock.patch.object(
                cj_module, "publish_promoted_operational_state_to_gcs", return_value=publication_b
            ):
                code, out, err = fx_a.run(["--control-file", str(control_file_a)])
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_terminal_manifest_lineage_mismatches_are_rejected(self) -> None:
        # A tampered manifest field is rejected either by
        # CompletedPromotedOperationalGCSPublication's own reconstruction
        # (identity/hash mismatch) or by the wrapper's own terminal-lineage
        # cross-check -- either way the wrapper must fail closed end-to-end.
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            for field, value in (
                ("bucket", "a-different-bucket"),
                ("preparation_id", "9" * 64),
                ("terminal_id", "9" * 64),
                ("advisory_id", "9" * 64),
            ):
                with self.subTest(field=field):
                    (base / field).mkdir(parents=True, exist_ok=True)
                    fx = _Fixture(base / field)
                    control_file, _terminal, publication = self._real_publication(fx)
                    object.__setattr__(publication.manifest, field, value)
                    with mock.patch.object(
                        cj_module, "publish_promoted_operational_state_to_gcs", return_value=publication
                    ):
                        code, out, err = fx.run(["--control-file", str(control_file)])
                    self.assertEqual(code, 2)
                    self.assertEqual(out, "")


class PyprojectRegistrationTests(unittest.TestCase):
    def test_pyproject_console_script_mapping_is_exact(self) -> None:
        pyproject_text = (
            Path(inspect.getfile(cj_module)).resolve().parent.parent.parent / "pyproject.toml"
        )
        text = pyproject_text.read_text(encoding="utf-8")
        self.assertIn(
            'india-swing-promoted-operational-cloud-job = "india_swing.promoted_operational_cloud_job:main"',
            text,
        )


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_discovery_retry_telegram_order_login_subprocess_or_deployment_capability(
        self,
    ) -> None:
        source = inspect.getsource(cj_module)
        for forbidden in (
            "glob(", "iterdir(", "listdir(", "rglob(", "while True",
            "for _ in itertools.count", "place_order", "modify_order", "cancel_order",
            "refresh_token", "import subprocess", "subprocess.", "import requests", "import urllib",
            "time.sleep",
        ):
            self.assertNotIn(forbidden, source)
        ast.parse(source)
        for forbidden_symbol in (
            "TelegramBotConfig", "TelegramDeliveryRequest", "deliver_telegram_notification",
            "UrllibTelegramHTTPTransport", "LocalTelegramDeliveryReceiptStore", "KiteLoginCredentials",
        ):
            self.assertFalse(hasattr(cj_module, forbidden_symbol))


if __name__ == "__main__":
    unittest.main()
