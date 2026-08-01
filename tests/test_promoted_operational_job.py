from __future__ import annotations

import ast
import inspect
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import timedelta
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_job as _job_module
from india_swing.identity import content_id
from india_swing.market_data.kite import KiteMarketDataAdapter
from india_swing.operations.portfolio_store import LocalSwingPortfolioArtifactStore
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_anchored_session import (
    run_publish_and_anchor_promoted_operational_session,
)
from india_swing.promoted_operational_assembly import (
    PromotedOperationalAssemblySpec,
    assemble_promoted_operational_runtime_inputs,
    encode_promoted_operational_assembly_spec,
)
from india_swing.promoted_operational_job import PromotedOperationalJobError, main
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
)
from india_swing.promoted_operational_runtime import (
    PinnedPromotedOperationalPortfolioSource,
    PromotedOperationalRuntimeState,
    build_promoted_operational_runtime_job_spec,
    run_promoted_operational_runtime_job,
)

from tests import test_promoted_operational_anchored_session as _anchored_tests
from tests import test_promoted_operational_assembly as _assembly_tests
from tests import test_promoted_operational_runner as _runner_tests


class _FakePreparationResolver:
    def __init__(self, preparation) -> None:
        self._preparation = preparation
        self.calls: list[str] = []

    def get(self, preparation_id: str):
        self.calls.append(preparation_id)
        return self._preparation


class _CountingCallable:
    def __init__(self, *, return_value=None) -> None:
        self.calls = 0
        self.return_value = return_value

    def __call__(self, *args, **kwargs):
        self.calls += 1
        return self.return_value


def _fixture(*, open_positions: int = 0):
    preparation, run_spec = _runner_tests._run_spec(chunk_size=500)
    fixture_adapter = KiteMarketDataAdapter(object(), sdk_version="test-fixture/1.0")
    expected_quote_source_id = content_id(fixture_adapter.identity_material, length=64)

    listing_keys = _assembly_tests._listing_keys(open_positions)
    as_of = run_spec.quote_gate_spec.decision_not_before - timedelta(seconds=10)
    portfolio = _assembly_tests._portfolio_snapshot(as_of=as_of, open_positions=open_positions)
    artifact = _assembly_tests._portfolio_artifact(portfolio)

    spec = PromotedOperationalAssemblySpec(
        preparation_id=preparation.manifest.preparation_id,
        portfolio_artifact_id=artifact.artifact_id,
        expected_portfolio_snapshot_id=portfolio.portfolio_snapshot_id,
        expected_quote_source_id=expected_quote_source_id,
        open_listing_keys=listing_keys,
        decision_not_before=run_spec.quote_gate_spec.decision_not_before,
        decision_deadline=run_spec.quote_gate_spec.decision_deadline,
        target_session=preparation.manifest.target_session,
        quote_gate_policy=run_spec.quote_gate_spec.policy,
        allocation_policy=run_spec.allocation_policy,
        maximum_quote_chunk_size=run_spec.maximum_quote_chunk_size,
        binding_bucket="test-bucket",
    )
    return preparation, run_spec, portfolio, artifact, spec


_ROOT_OPTIONS = (
    "--reference-root",
    "--identity-evidence-root",
    "--calendar-root",
    "--daily-reports-root",
    "--historical-corpus-root",
    "--promoted-root",
    "--graph-publication-root",
    "--engine-run-root",
    "--research-run-root",
    "--operational-preparation-root",
)


class PromotedOperationalJobTests(unittest.TestCase):
    def _run_main(self, argv, **kwargs):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(argv, **kwargs)
        return code, stdout.getvalue(), stderr.getvalue()

    def _prepare_roots(self, tmp_path: Path, spec, artifact):
        portfolio_artifact_root = tmp_path / "portfolio-artifact"
        LocalSwingPortfolioArtifactStore(portfolio_artifact_root).put(artifact)

        state_root = tmp_path / "state"
        assembly_spec_file = tmp_path / "assembly.json"
        assembly_spec_file.write_bytes(encode_promoted_operational_assembly_spec(spec))

        argv = ["--assembly-spec-file", str(assembly_spec_file)]
        for option in _ROOT_OPTIONS:
            argv.extend([option, str(tmp_path / option.strip("-"))])
        argv.extend(["--portfolio-artifact-root", str(portfolio_artifact_root)])
        argv.extend(["--state-root", str(state_root)])
        return argv, state_root

    def test_parser_rejects_missing_duplicate_unknown_relative_and_traversing_arguments(
        self,
    ) -> None:
        root = Path(tempfile.gettempdir()) / "promoted-operational-job-parser-test"
        valid_values = {option: str(root / option.strip("-")) for option in _job_module._OPTIONS}

        def _argv(values: dict[str, str]) -> list[str]:
            result: list[str] = []
            for option, value in values.items():
                result.extend([option, value])
            return result

        with self.subTest(case="missing"):
            values = dict(valid_values)
            del values["--state-root"]
            code, out, err = self._run_main(_argv(values), environ={})
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertEqual(
                json.loads(err), {"error_type": "PromotedOperationalJobError", "status": "FAILED"}
            )

        with self.subTest(case="duplicate"):
            argv = _argv(valid_values) + ["--state-root", str(root / "other")]
            code, out, err = self._run_main(argv, environ={})
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

        with self.subTest(case="unknown"):
            argv = _argv(valid_values) + ["--unknown-option", str(root / "x")]
            code, out, err = self._run_main(argv, environ={})
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

        with self.subTest(case="relative"):
            values = dict(valid_values)
            values["--state-root"] = "relative-state-root"
            code, out, err = self._run_main(_argv(values), environ={})
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

        with self.subTest(case="traversing"):
            values = dict(valid_values)
            values["--state-root"] = str(root / ".." / "escape")
            code, out, err = self._run_main(_argv(values), environ={})
            self.assertEqual(code, 2)
            self.assertEqual(out, "")

    def test_early_failures_occur_before_gcs_factory_state_stores_or_runtime_callable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            preparation, run_spec, portfolio, artifact, spec = _fixture()
            argv, state_root = self._prepare_roots(tmp_path, spec, artifact)
            secret = "SECRET-PLANTED-MARKER-9f1a"

            with self.subTest(case="missing_assembly_spec_file"):
                gcs_factory = _CountingCallable(return_value=object())
                runtime_callable = _CountingCallable(return_value=None)
                broken_argv = list(argv)
                index = broken_argv.index("--assembly-spec-file")
                broken_argv[index + 1] = str(tmp_path / "missing.json")
                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        broken_argv,
                        environ={
                            "INDIA_SWING_KITE_API_KEY": secret,
                            "INDIA_SWING_KITE_ACCESS_TOKEN": secret,
                        },
                        gcs_client_factory=gcs_factory,
                        runtime_callable=runtime_callable,
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertNotIn(secret, err)
                self.assertEqual(gcs_factory.calls, 0)
                self.assertEqual(runtime_callable.calls, 0)

            with self.subTest(case="missing_credentials"):
                gcs_factory = _CountingCallable(return_value=object())
                runtime_callable = _CountingCallable(return_value=None)
                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        argv,
                        environ={},
                        gcs_client_factory=gcs_factory,
                        runtime_callable=runtime_callable,
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertEqual(gcs_factory.calls, 0)
                self.assertEqual(runtime_callable.calls, 0)

            with self.subTest(case="quote_source_identity_mismatch"):
                gcs_factory = _CountingCallable(return_value=object())
                runtime_callable = _CountingCallable(return_value=None)

                def _wrong_adapter_factory(credentials, clock):
                    return KiteMarketDataAdapter(
                        object(), sdk_version="wrong-fixture/1.0", clock=clock
                    )

                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        argv,
                        environ={
                            "INDIA_SWING_KITE_API_KEY": secret,
                            "INDIA_SWING_KITE_ACCESS_TOKEN": secret,
                        },
                        kite_adapter_factory=_wrong_adapter_factory,
                        gcs_client_factory=gcs_factory,
                        runtime_callable=runtime_callable,
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertNotIn(secret, err)
                self.assertEqual(gcs_factory.calls, 0)
                self.assertEqual(runtime_callable.calls, 0)

    def test_valid_composition_shares_one_gcs_client_and_calls_runtime_exactly_once(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            preparation, run_spec, portfolio, artifact, spec = _fixture()
            argv, state_root = self._prepare_roots(tmp_path, spec, artifact)

            fake_client = object()
            gcs_calls: list[int] = []

            def _gcs_factory():
                gcs_calls.append(1)
                return fake_client

            backend = _anchored_tests._FakeBindingBackend()
            preflight = _anchored_tests._PermissiveRecordingPreflight()

            def _make_runtime_callable(*, quote_source):
                def _fake(**kwargs):
                    received_quote_source = kwargs["quote_source"]
                    self.assertEqual(
                        received_quote_source.source_id, spec.expected_quote_source_id
                    )
                    anchored = run_publish_and_anchor_promoted_operational_session(
                        spec=kwargs["run_spec"],
                        quote_source=quote_source,
                        portfolio_source=kwargs["portfolio_source"],
                        clock=kwargs["clock"],
                        advisory_outbox=kwargs["advisory_outbox"],
                        terminal_store=kwargs["terminal_store"],
                        paper_ledger=kwargs["paper_ledger"],
                        binding_bucket=kwargs["job_spec"].binding_bucket,
                        binding_writer=backend,
                        binding_reader=backend,
                        binding_preflight=preflight,
                    )
                    self._last_call_kwargs = kwargs
                    return PromotedOperationalRuntimeState(
                        job_spec=kwargs["job_spec"], anchored=anchored
                    )

                return _fake

            fresh_quote_source = _runner_tests._FakeQuoteSource(
                responder=_runner_tests._sorted_quote_responder(preparation),
                source_id=spec.expected_quote_source_id,
            )

            with mock.patch.object(
                _job_module,
                "build_promoted_operational_preparation_store",
                return_value=(None, _FakePreparationResolver(preparation)),
            ):
                code, out, err = self._run_main(
                    argv,
                    environ={
                        "INDIA_SWING_KITE_API_KEY": "k",
                        "INDIA_SWING_KITE_ACCESS_TOKEN": "t",
                    },
                    kite_adapter_factory=lambda creds, clk: KiteMarketDataAdapter(
                        object(), sdk_version="test-fixture/1.0", clock=clk
                    ),
                    gcs_client_factory=_gcs_factory,
                    runtime_callable=_make_runtime_callable(quote_source=fresh_quote_source),
                )

            self.assertEqual(err, "")
            self.assertEqual(code, 0)
            self.assertEqual(len(gcs_calls), 1)
            call_kwargs = self._last_call_kwargs
            self.assertIs(call_kwargs["binding_writer"]._client, fake_client)
            self.assertIs(call_kwargs["binding_reader"]._client, fake_client)
            self.assertIs(call_kwargs["binding_preflight"], call_kwargs["binding_reader"])
            self.assertEqual(call_kwargs["job_spec"].binding_bucket, "test-bucket")

            envelope = json.loads(out)
            self.assertEqual(
                set(envelope),
                {
                    "status",
                    "assembly_spec_id",
                    "runtime_job_spec_id",
                    "operational_run_spec_id",
                    "preparation_id",
                    "target_session",
                    "terminal_id",
                    "terminal_status",
                    "action",
                    "failure_codes",
                    "advisory_id",
                    "binding_id",
                    "binding_generation",
                    "reused_existing_terminal",
                    "paper_only",
                    "notification_eligible",
                    "execution_eligible",
                },
            )
            self.assertEqual(envelope["status"], "PROMOTED_OPERATIONAL_JOB_COMPLETE")
            self.assertFalse(envelope["reused_existing_terminal"])
            self.assertIs(envelope["paper_only"], True)
            self.assertIs(envelope["notification_eligible"], False)
            self.assertIs(envelope["execution_eligible"], False)
            self.assertIsInstance(envelope["failure_codes"], list)

            def _exploding(_keys=None):
                raise AssertionError("quote acquisition must not happen on replay")

            exploding_quote_source = _runner_tests._FakeQuoteSource(
                responder=_exploding, source_id=spec.expected_quote_source_id
            )

            with mock.patch.object(
                _job_module,
                "build_promoted_operational_preparation_store",
                return_value=(None, _FakePreparationResolver(preparation)),
            ):
                second_code, second_out, second_err = self._run_main(
                    argv,
                    environ={
                        "INDIA_SWING_KITE_API_KEY": "k",
                        "INDIA_SWING_KITE_ACCESS_TOKEN": "t",
                    },
                    kite_adapter_factory=lambda creds, clk: KiteMarketDataAdapter(
                        object(), sdk_version="test-fixture/1.0", clock=clk
                    ),
                    gcs_client_factory=_gcs_factory,
                    runtime_callable=_make_runtime_callable(quote_source=exploding_quote_source),
                )
            self.assertEqual(second_err, "")
            self.assertEqual(second_code, 0)
            second_envelope = json.loads(second_out)
            self.assertTrue(second_envelope["reused_existing_terminal"])
            self.assertEqual(second_envelope["terminal_id"], envelope["terminal_id"])
            self.assertEqual(second_envelope["binding_id"], envelope["binding_id"])

    def test_malicious_or_mismatched_runtime_result_never_produces_a_false_success(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            preparation, run_spec, portfolio, artifact, spec = _fixture()
            argv, state_root = self._prepare_roots(tmp_path, spec, artifact)

            common_kwargs = dict(
                environ={"INDIA_SWING_KITE_API_KEY": "k", "INDIA_SWING_KITE_ACCESS_TOKEN": "t"},
                kite_adapter_factory=lambda creds, clk: KiteMarketDataAdapter(
                    object(), sdk_version="test-fixture/1.0", clock=clk
                ),
                gcs_client_factory=lambda: object(),
            )

            with self.subTest(case="wrong_type"):
                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        argv, runtime_callable=lambda **kwargs: object(), **common_kwargs
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

            with self.subTest(case="mismatched_job_spec_from_an_unrelated_run"):
                other_preparation, other_run_spec = _runner_tests._run_spec(chunk_size=1)
                other_quote_source = _runner_tests._FakeQuoteSource(
                    responder=_runner_tests._sorted_quote_responder(other_preparation),
                    source_id=other_run_spec.expected_quote_source_id,
                )
                other_portfolio_context = _runner_tests._portfolio_context(
                    as_of=_runner_tests._PORTFOLIO_AS_OF
                )
                other_portfolio_source = PinnedPromotedOperationalPortfolioSource(
                    other_portfolio_context
                )
                other_job_spec = build_promoted_operational_runtime_job_spec(
                    run_spec=other_run_spec,
                    portfolio_context=other_portfolio_context,
                    binding_bucket="other-bucket",
                )
                other_clock = _runner_tests._clock(
                    _runner_tests._STARTED_AT,
                    _runner_tests._RUN_EVALUATED_AT,
                    _runner_tests._COMPLETED_AT,
                )
                other_backend = _anchored_tests._FakeBindingBackend()
                other_preflight = _anchored_tests._PermissiveRecordingPreflight()
                with tempfile.TemporaryDirectory() as other_directory:
                    other_root = Path(other_directory)
                    other_state = run_promoted_operational_runtime_job(
                        job_spec=other_job_spec,
                        run_spec=other_run_spec,
                        quote_source=other_quote_source,
                        portfolio_source=other_portfolio_source,
                        clock=other_clock,
                        advisory_outbox=LocalPromotedOperationalAdvisoryOutbox(other_root),
                        terminal_store=LocalPromotedOperationalTerminalStore(other_root / "runner"),
                        paper_ledger=LocalPaperTradeLedger(other_root / "paper"),
                        binding_writer=other_backend,
                        binding_reader=other_backend,
                        binding_preflight=other_preflight,
                    )

                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        argv, runtime_callable=lambda **kwargs: other_state, **common_kwargs
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

    def test_adversarial_result_mutations_never_produce_a_false_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            preparation, run_spec, portfolio, artifact, spec = _fixture()
            argv, state_root = self._prepare_roots(tmp_path, spec, artifact)
            secret = "SECRET-PLANTED-TAMPER-MARKER-4d2e"

            # Independently compute the exact same assembly main() will
            # compute internally, so a genuinely valid base result can be
            # built whose job_spec matches main()'s own byte-for-byte.
            assembly = assemble_promoted_operational_runtime_inputs(
                spec=spec,
                preparation_resolver=_FakePreparationResolver(preparation),
                portfolio_artifact_resolver=LocalSwingPortfolioArtifactStore(
                    tmp_path / "portfolio-artifact"
                ),
            )

            def _build_valid_state():
                with tempfile.TemporaryDirectory() as state_dir:
                    state_dir_path = Path(state_dir)
                    quote_source = _runner_tests._FakeQuoteSource(
                        responder=_runner_tests._sorted_quote_responder(preparation),
                        source_id=assembly.runtime_job_spec.expected_quote_source_id,
                    )
                    portfolio_source = PinnedPromotedOperationalPortfolioSource(
                        assembly.portfolio_context
                    )
                    backend = _anchored_tests._FakeBindingBackend()
                    preflight = _anchored_tests._PermissiveRecordingPreflight()
                    clock = _runner_tests._clock(
                        _runner_tests._STARTED_AT,
                        _runner_tests._RUN_EVALUATED_AT,
                        _runner_tests._COMPLETED_AT,
                    )
                    return run_promoted_operational_runtime_job(
                        job_spec=assembly.runtime_job_spec,
                        run_spec=assembly.run_spec,
                        quote_source=quote_source,
                        portfolio_source=portfolio_source,
                        clock=clock,
                        advisory_outbox=LocalPromotedOperationalAdvisoryOutbox(state_dir_path),
                        terminal_store=LocalPromotedOperationalTerminalStore(
                            state_dir_path / "runner"
                        ),
                        paper_ledger=LocalPaperTradeLedger(state_dir_path / "paper"),
                        binding_writer=backend,
                        binding_reader=backend,
                        binding_preflight=preflight,
                    )

            def _run_with_result(result):
                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    return self._run_main(
                        argv,
                        environ={
                            "INDIA_SWING_KITE_API_KEY": secret,
                            "INDIA_SWING_KITE_ACCESS_TOKEN": secret,
                        },
                        kite_adapter_factory=lambda creds, clk: KiteMarketDataAdapter(
                            object(), sdk_version="test-fixture/1.0", clock=clk
                        ),
                        gcs_client_factory=lambda: object(),
                        runtime_callable=lambda **kwargs: result,
                    )

            with self.subTest(case="baseline_accepted"):
                state = _build_valid_state()
                code, out, err = _run_with_result(state)
                self.assertEqual(code, 0)
                self.assertEqual(err, "")

            with self.subTest(case="advisory_id_tampered_stale_terminal_id"):
                state = _build_valid_state()
                object.__setattr__(state.anchored.published.terminal, "advisory_id", secret)
                code, out, err = _run_with_result(state)
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertNotIn(secret, err)

            with self.subTest(case="terminal_status_tampered_stale_terminal_id"):
                state = _build_valid_state()
                terminal = state.anchored.published.terminal
                other_status = next(
                    value for value in type(terminal.status) if value is not terminal.status
                )
                object.__setattr__(terminal, "status", other_status)
                code, out, err = _run_with_result(state)
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

            with self.subTest(case="binding_preparation_id_tampered_stale_binding_id"):
                state = _build_valid_state()
                object.__setattr__(state.anchored.binding_record, "preparation_id", "1" * 64)
                code, out, err = _run_with_result(state)
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

            with self.subTest(case="binding_expected_terminal_id_tampered_stale_binding_id"):
                state = _build_valid_state()
                object.__setattr__(state.anchored.binding_record, "expected_terminal_id", "2" * 64)
                code, out, err = _run_with_result(state)
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

            with self.subTest(case="binding_bucket_tampered"):
                state = _build_valid_state()
                object.__setattr__(state.anchored, "binding_bucket", secret)
                code, out, err = _run_with_result(state)
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertNotIn(secret, err)

            with self.subTest(case="binding_object_name_tampered"):
                state = _build_valid_state()
                object.__setattr__(state.anchored, "binding_object_name", secret)
                code, out, err = _run_with_result(state)
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertNotIn(secret, err)

            with self.subTest(case="published_reused_existing_terminal_tampered"):
                state = _build_valid_state()
                object.__setattr__(
                    state.anchored.published,
                    "reused_existing_terminal",
                    not state.anchored.published.reused_existing_terminal,
                )
                code, out, err = _run_with_result(state)
                self.assertEqual(code, 2)
                self.assertEqual(out, "")

    def test_dependency_preflight_rejects_noncallable_seams_and_none_gcs_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            preparation, run_spec, portfolio, artifact, spec = _fixture()
            argv, state_root = self._prepare_roots(tmp_path, spec, artifact)

            with self.subTest(case="non_callable_clock"):
                gcs_factory = _CountingCallable(return_value=object())
                runtime_callable = _CountingCallable(return_value=None)
                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        argv,
                        environ={
                            "INDIA_SWING_KITE_API_KEY": "k",
                            "INDIA_SWING_KITE_ACCESS_TOKEN": "t",
                        },
                        clock="not-callable",
                        gcs_client_factory=gcs_factory,
                        runtime_callable=runtime_callable,
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertEqual(gcs_factory.calls, 0)
                self.assertEqual(runtime_callable.calls, 0)

            with self.subTest(case="non_callable_runtime_callable"):
                gcs_factory = _CountingCallable(return_value=object())
                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        argv,
                        environ={
                            "INDIA_SWING_KITE_API_KEY": "k",
                            "INDIA_SWING_KITE_ACCESS_TOKEN": "t",
                        },
                        gcs_client_factory=gcs_factory,
                        runtime_callable="not-callable",
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertEqual(gcs_factory.calls, 0)

            with self.subTest(case="non_callable_kite_adapter_factory"):
                gcs_factory = _CountingCallable(return_value=object())
                runtime_callable = _CountingCallable(return_value=None)
                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        argv,
                        environ={
                            "INDIA_SWING_KITE_API_KEY": "k",
                            "INDIA_SWING_KITE_ACCESS_TOKEN": "t",
                        },
                        kite_adapter_factory="not-callable",
                        gcs_client_factory=gcs_factory,
                        runtime_callable=runtime_callable,
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertEqual(gcs_factory.calls, 0)
                self.assertEqual(runtime_callable.calls, 0)

            with self.subTest(case="gcs_factory_returns_none"):
                gcs_factory = _CountingCallable(return_value=None)
                runtime_callable = _CountingCallable(return_value=None)
                with mock.patch.object(
                    _job_module,
                    "build_promoted_operational_preparation_store",
                    return_value=(None, _FakePreparationResolver(preparation)),
                ):
                    code, out, err = self._run_main(
                        argv,
                        environ={
                            "INDIA_SWING_KITE_API_KEY": "k",
                            "INDIA_SWING_KITE_ACCESS_TOKEN": "t",
                        },
                        kite_adapter_factory=lambda creds, clk: KiteMarketDataAdapter(
                            object(), sdk_version="test-fixture/1.0", clock=clk
                        ),
                        gcs_client_factory=gcs_factory,
                        runtime_callable=runtime_callable,
                    )
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertEqual(gcs_factory.calls, 1)
                self.assertEqual(runtime_callable.calls, 0)

    def test_pyproject_console_script_mapping_is_exact(self) -> None:
        pyproject_text = Path(
            inspect.getfile(_job_module)
        ).resolve().parent.parent.parent / "pyproject.toml"
        text = pyproject_text.read_text(encoding="utf-8")
        self.assertIn(
            'india-swing-promoted-operational-job = "india_swing.promoted_operational_job:main"',
            text,
        )

    def test_module_has_no_discovery_retry_telegram_order_login_subprocess_or_deployment_capability(
        self,
    ) -> None:
        source = inspect.getsource(_job_module)
        for forbidden in (
            "glob(",
            "iterdir(",
            "listdir(",
            "rglob(",
            "while True",
            "for _ in itertools.count",
            "place_order",
            "modify_order",
            "cancel_order",
            "refresh_token",
            "import subprocess",
            "subprocess.",
            "import requests",
            "import urllib",
        ):
            self.assertNotIn(forbidden, source)
        ast.parse(source)

        # The module docstring legitimately explains, in prose, what this
        # entrypoint deliberately does NOT do (Telegram, interactive/
        # browser login, deployment) -- a plain substring scan over that
        # prose would false-positive. Instead assert the actual forbidden
        # capabilities/symbols are never imported or defined at all.
        for forbidden_symbol in (
            "TelegramBotConfig",
            "TelegramDeliveryRequest",
            "deliver_telegram_notification",
            "UrllibTelegramHTTPTransport",
            "LocalTelegramDeliveryReceiptStore",
            "KiteLoginCredentials",
        ):
            self.assertFalse(hasattr(_job_module, forbidden_symbol))


if __name__ == "__main__":
    unittest.main()
