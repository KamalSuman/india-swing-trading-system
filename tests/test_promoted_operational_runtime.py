from __future__ import annotations

import ast
import inspect
import json
import tempfile
import unittest
from dataclasses import replace as _dc_replace
from datetime import timedelta
from pathlib import Path

import india_swing.promoted_operational_runtime as _runtime_module
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_allocation import PromotedOperationalPortfolioContext
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
)
from india_swing.promoted_operational_runtime import (
    MAXIMUM_RUNTIME_JOB_SPEC_BYTES,
    PROMOTED_OPERATIONAL_RUNTIME_JOB_SPEC_SCHEMA_VERSION,
    PinnedPromotedOperationalPortfolioSource,
    PromotedOperationalRuntimeError,
    PromotedOperationalRuntimeState,
    build_promoted_operational_runtime_job_spec,
    decode_promoted_operational_runtime_job_spec,
    encode_promoted_operational_runtime_job_spec,
    load_promoted_operational_runtime_job_spec_file,
    run_promoted_operational_runtime_job,
)

from tests import test_promoted_operational_anchored_session as _anchored_tests
from tests import test_promoted_operational_runner as _runner_tests


def _reencode(root: dict) -> bytes:
    return (
        json.dumps(root, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


class _AssertingQuoteSource:
    """A quote source whose identity is readable but whose acquisition
    method must never be called -- used to prove the orchestrator fails
    closed before any quote acquisition."""

    def __init__(self, *, source_id: str) -> None:
        self._source_id = source_id

    @property
    def source_id(self) -> str:
        return self._source_id

    def fetch_full_quotes(self, listing_keys):
        raise AssertionError("fetch_full_quotes must not be called on a mismatch")


class _IdentityExplodingQuoteSource:
    """A quote source whose identity property itself raises -- used to
    prove a check runs strictly before any quote-source identity read at
    all (not just before acquisition)."""

    @property
    def source_id(self) -> str:
        raise AssertionError("source_id must not be read before earlier validation completes")

    def fetch_full_quotes(self, listing_keys):
        raise AssertionError("fetch_full_quotes must not be called")


class _AssertingPreflight:
    def verify_bucket_reachable(self, *, bucket: str) -> None:
        raise AssertionError("preflight must not be called on a mismatch")


class _AssertingBindingBackend:
    def create_or_verify(self, **_kwargs):
        raise AssertionError("writer must not be called on a mismatch")

    def read_current(self, **_kwargs):
        raise AssertionError("reader must not be called on a mismatch")

    def read_current_optional(self, **_kwargs):
        raise AssertionError("reader must not be called on a mismatch")


def _asserting_clock():
    raise AssertionError("clock must not be called on a mismatch")


class PromotedOperationalRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        root = Path(self._temp.name)
        self.advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(root)
        self.terminal_store = LocalPromotedOperationalTerminalStore(root / "runner")
        self.paper_ledger = LocalPaperTradeLedger(root / "paper")

    def _fixture(self):
        preparation, run_spec = _runner_tests._run_spec(chunk_size=500)
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_context = _runner_tests._portfolio_context(as_of=_runner_tests._PORTFOLIO_AS_OF)
        portfolio_source = PinnedPromotedOperationalPortfolioSource(portfolio_context)
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        job_spec = build_promoted_operational_runtime_job_spec(
            run_spec=run_spec, portfolio_context=portfolio_context, binding_bucket="test-bucket"
        )
        backend = _anchored_tests._FakeBindingBackend()
        preflight = _anchored_tests._PermissiveRecordingPreflight()
        return (
            preparation,
            run_spec,
            quote_source,
            portfolio_context,
            portfolio_source,
            clock,
            job_spec,
            backend,
            preflight,
        )

    def test_builder_derives_every_field_exactly_and_rejects_mismatched_portfolio_source(
        self,
    ) -> None:
        _, run_spec, _, portfolio_context, _, _, job_spec, _, _ = self._fixture()
        manifest = run_spec.quote_gate_spec.preparation.manifest
        self.assertEqual(job_spec.operational_run_spec_id, run_spec.spec_id)
        self.assertEqual(job_spec.preparation_id, manifest.preparation_id)
        self.assertEqual(job_spec.target_session, manifest.target_session)
        self.assertEqual(job_spec.decision_not_before, run_spec.quote_gate_spec.decision_not_before)
        self.assertEqual(job_spec.decision_deadline, run_spec.quote_gate_spec.decision_deadline)
        self.assertEqual(job_spec.expected_quote_source_id, run_spec.expected_quote_source_id)
        self.assertEqual(
            job_spec.expected_portfolio_source_id, run_spec.expected_portfolio_source_id
        )
        self.assertEqual(job_spec.expected_portfolio_context_id, portfolio_context.context_id)
        self.assertEqual(job_spec.binding_bucket, "test-bucket")
        self.assertIs(job_spec.paper_only, True)
        self.assertIs(job_spec.notification_eligible, False)
        self.assertIs(job_spec.execution_eligible, False)
        self.assertEqual(
            job_spec.schema_version, PROMOTED_OPERATIONAL_RUNTIME_JOB_SPEC_SCHEMA_VERSION
        )

        mismatched_context = PromotedOperationalPortfolioContext(
            portfolio=portfolio_context.portfolio,
            source_portfolio_artifact_id="6" * 64,
            open_listing_keys=portfolio_context.open_listing_keys,
        )
        self.assertNotEqual(
            mismatched_context.source_portfolio_artifact_id, run_spec.expected_portfolio_source_id
        )
        with self.assertRaises(PromotedOperationalRuntimeError):
            build_promoted_operational_runtime_job_spec(
                run_spec=run_spec, portfolio_context=mismatched_context, binding_bucket="test-bucket"
            )

    def test_codec_round_trips_canonical_bytes_and_rejects_malformed_or_tampered_payloads(
        self,
    ) -> None:
        *_, job_spec, _, _ = self._fixture()
        payload = encode_promoted_operational_runtime_job_spec(job_spec)
        decoded = decode_promoted_operational_runtime_job_spec(payload)
        self.assertEqual(decoded, job_spec)
        self.assertEqual(encode_promoted_operational_runtime_job_spec(decoded), payload)

        with self.subTest(case="duplicate_key"):
            needle = b'"paper_only":true,'
            self.assertEqual(payload.count(needle), 1)
            duplicated = payload.replace(needle, needle + needle, 1)
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(duplicated)

        with self.subTest(case="unknown_key"):
            root = json.loads(payload)
            root["runtime_job_spec"]["unexpected"] = "x"
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(_reencode(root))

        with self.subTest(case="missing_key"):
            root = json.loads(payload)
            del root["runtime_job_spec"]["binding_bucket"]
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(_reencode(root))

        text = payload.decode("utf-8")
        id_needle = f'"job_spec_id":"{job_spec.job_spec_id}"'
        self.assertIn(id_needle, text)

        with self.subTest(case="float_value"):
            mutated = text.replace(id_needle, '"job_spec_id":1.5', 1).encode("utf-8")
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(mutated)

        with self.subTest(case="nan_value"):
            mutated = text.replace(id_needle, '"job_spec_id":NaN', 1).encode("utf-8")
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(mutated)

        with self.subTest(case="infinity_value"):
            mutated = text.replace(id_needle, '"job_spec_id":Infinity', 1).encode("utf-8")
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(mutated)

        with self.subTest(case="naive_datetime"):
            root = json.loads(payload)
            root["runtime_job_spec"]["decision_not_before"] = "2026-01-01T09:15:00"
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(_reencode(root))

        with self.subTest(case="noncanonical_datetime_offset"):
            root = json.loads(payload)
            ist_value = job_spec.decision_not_before.astimezone(INDIA_STANDARD_TIME)
            root["runtime_job_spec"]["decision_not_before"] = ist_value.isoformat()
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(_reencode(root))

        with self.subTest(case="direct_construction_rejects_nonzero_offset"):
            ist_value = job_spec.decision_not_before.astimezone(INDIA_STANDARD_TIME)
            self.assertNotEqual(ist_value.utcoffset(), timedelta(0))
            with self.assertRaises(PromotedOperationalRuntimeError):
                _dc_replace(job_spec, decision_not_before=ist_value)
            # Canonical UTC construction and codec round-trip remain exact.
            self.assertEqual(job_spec.decision_not_before.utcoffset(), timedelta(0))
            reconstructed = _dc_replace(job_spec, decision_not_before=job_spec.decision_not_before)
            self.assertEqual(reconstructed, job_spec)
            self.assertEqual(encode_promoted_operational_runtime_job_spec(reconstructed), payload)

        with self.subTest(case="invalid_authority"):
            root = json.loads(payload)
            root["runtime_job_spec"]["notification_eligible"] = True
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(_reencode(root))

        with self.subTest(case="oversized"):
            oversized = payload + b" " * MAXIMUM_RUNTIME_JOB_SPEC_BYTES
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(oversized)

        with self.subTest(case="tampered_field_stale_id"):
            root = json.loads(payload)
            root["runtime_job_spec"]["binding_bucket"] = "a-different-bucket"
            with self.assertRaises(PromotedOperationalRuntimeError):
                decode_promoted_operational_runtime_job_spec(_reencode(root))

    def test_file_loader_round_trips_and_rejects_unsafe_or_invalid_inputs(self) -> None:
        *_, job_spec, _, _ = self._fixture()
        payload = encode_promoted_operational_runtime_job_spec(job_spec)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            good_path = root / "job.json"
            good_path.write_bytes(payload)
            self.assertEqual(load_promoted_operational_runtime_job_spec_file(good_path), job_spec)

            with self.subTest(case="relative_path"):
                with self.assertRaises(PromotedOperationalRuntimeError):
                    load_promoted_operational_runtime_job_spec_file(Path("job.json"))

            with self.subTest(case="concrete_path_subclass_rejected"):
                class _OverridingPath(_runtime_module._CONCRETE_PATH_TYPE):
                    def is_absolute(self) -> bool:
                        raise AssertionError(
                            "overridden path behavior must never be consulted"
                        )

                subclassed_path = _OverridingPath(good_path)
                self.assertIsInstance(subclassed_path, _runtime_module._CONCRETE_PATH_TYPE)
                self.assertIsNot(type(subclassed_path), _runtime_module._CONCRETE_PATH_TYPE)
                with self.assertRaises(PromotedOperationalRuntimeError):
                    load_promoted_operational_runtime_job_spec_file(subclassed_path)

            with self.subTest(case="traversing_path"):
                traversing = root / ".." / root.name / "job.json"
                with self.assertRaises(PromotedOperationalRuntimeError):
                    load_promoted_operational_runtime_job_spec_file(traversing)

            with self.subTest(case="missing_path"):
                with self.assertRaises(PromotedOperationalRuntimeError):
                    load_promoted_operational_runtime_job_spec_file(root / "missing.json")

            with self.subTest(case="directory_path"):
                with self.assertRaises(PromotedOperationalRuntimeError):
                    load_promoted_operational_runtime_job_spec_file(root)

            with self.subTest(case="tampered_content"):
                tampered_path = root / "tampered.json"
                tampered_path.write_bytes(
                    payload.replace(job_spec.job_spec_id.encode(), b"0" * 64)
                )
                with self.assertRaises(PromotedOperationalRuntimeError):
                    load_promoted_operational_runtime_job_spec_file(tampered_path)

            with self.subTest(case="oversized_file"):
                oversized_path = root / "oversized.json"
                oversized_path.write_bytes(payload + b" " * MAXIMUM_RUNTIME_JOB_SPEC_BYTES)
                with self.assertRaises(PromotedOperationalRuntimeError):
                    load_promoted_operational_runtime_job_spec_file(oversized_path)

            with self.subTest(case="symlink_path"):
                symlink_path = root / "link.json"
                try:
                    symlink_path.symlink_to(good_path)
                except OSError:
                    self.skipTest("symlinks not permitted in this environment")
                with self.assertRaises(PromotedOperationalRuntimeError):
                    load_promoted_operational_runtime_job_spec_file(symlink_path)

    def test_pinned_portfolio_source_exposes_identity_and_rejects_wrong_type_and_tampering(
        self,
    ) -> None:
        _, _, _, portfolio_context, portfolio_source, _, _, _, _ = self._fixture()
        self.assertEqual(portfolio_source.source_id, portfolio_context.source_portfolio_artifact_id)
        self.assertEqual(portfolio_source.context_id, portfolio_context.context_id)
        self.assertEqual(portfolio_source.read_portfolio_context(), portfolio_context)

        with self.assertRaises(PromotedOperationalRuntimeError):
            PinnedPromotedOperationalPortfolioSource(object())

        tampered_context = PromotedOperationalPortfolioContext(
            portfolio=portfolio_context.portfolio,
            source_portfolio_artifact_id=portfolio_context.source_portfolio_artifact_id,
            open_listing_keys=portfolio_context.open_listing_keys,
        )
        source = PinnedPromotedOperationalPortfolioSource(tampered_context)
        object.__setattr__(tampered_context, "context_id", "0" * 64)
        with self.assertRaises(PromotedOperationalRuntimeError):
            _ = source.context_id
        with self.assertRaises(PromotedOperationalRuntimeError):
            _ = source.source_id
        with self.assertRaises(PromotedOperationalRuntimeError):
            source.read_portfolio_context()

    def test_orchestrator_rejects_every_mismatch_before_any_side_effect(self) -> None:
        (
            _,
            run_spec,
            _,
            portfolio_context,
            portfolio_source,
            _,
            job_spec,
            _,
            _,
        ) = self._fixture()

        common_kwargs = dict(
            run_spec=run_spec,
            clock=_asserting_clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_writer=_AssertingBindingBackend(),
            binding_reader=_AssertingBindingBackend(),
            binding_preflight=_AssertingPreflight(),
        )

        mismatched_job_specs = {
            "operational_run_spec_id": _dc_replace(job_spec, operational_run_spec_id="0" * 64),
            "preparation_id": _dc_replace(job_spec, preparation_id="1" * 64),
            "expected_quote_source_id": _dc_replace(job_spec, expected_quote_source_id="2" * 64),
            "expected_portfolio_source_id": _dc_replace(
                job_spec, expected_portfolio_source_id="3" * 64
            ),
            "expected_portfolio_context_id": _dc_replace(
                job_spec, expected_portfolio_context_id="4" * 64
            ),
            "decision_not_before": _dc_replace(
                job_spec, decision_not_before=job_spec.decision_not_before + timedelta(seconds=1)
            ),
            "decision_deadline": _dc_replace(
                job_spec, decision_deadline=job_spec.decision_deadline + timedelta(seconds=1)
            ),
            "target_session": _dc_replace(
                job_spec,
                target_session=job_spec.target_session - timedelta(days=1),
                decision_not_before=job_spec.decision_not_before - timedelta(days=1),
                decision_deadline=job_spec.decision_deadline - timedelta(days=1),
            ),
        }
        for case, mismatched_job_spec in mismatched_job_specs.items():
            with self.subTest(case=case):
                with self.assertRaises(PromotedOperationalRuntimeError):
                    run_promoted_operational_runtime_job(
                        job_spec=mismatched_job_spec,
                        quote_source=_AssertingQuoteSource(source_id=run_spec.expected_quote_source_id),
                        portfolio_source=portfolio_source,
                        **common_kwargs,
                    )

        with self.subTest(case="quote_source_id_mismatch"):
            with self.assertRaises(PromotedOperationalRuntimeError):
                run_promoted_operational_runtime_job(
                    job_spec=job_spec,
                    quote_source=_AssertingQuoteSource(source_id="5" * 64),
                    portfolio_source=portfolio_source,
                    **common_kwargs,
                )

        with self.subTest(case="portfolio_source_id_mismatch"):
            foreign_context = PromotedOperationalPortfolioContext(
                portfolio=portfolio_context.portfolio,
                source_portfolio_artifact_id="6" * 64,
                open_listing_keys=portfolio_context.open_listing_keys,
            )
            foreign_source = PinnedPromotedOperationalPortfolioSource(foreign_context)
            with self.assertRaises(PromotedOperationalRuntimeError):
                run_promoted_operational_runtime_job(
                    job_spec=job_spec,
                    quote_source=_AssertingQuoteSource(source_id=run_spec.expected_quote_source_id),
                    portfolio_source=foreign_source,
                    **common_kwargs,
                )

        with self.subTest(case="portfolio_context_id_mismatch"):
            alt_context = _runner_tests._portfolio_context(
                as_of=_runner_tests._PORTFOLIO_AS_OF - timedelta(seconds=1)
            )
            self.assertEqual(
                alt_context.source_portfolio_artifact_id, run_spec.expected_portfolio_source_id
            )
            self.assertNotEqual(alt_context.context_id, job_spec.expected_portfolio_context_id)
            alt_source = PinnedPromotedOperationalPortfolioSource(alt_context)
            with self.assertRaises(PromotedOperationalRuntimeError):
                run_promoted_operational_runtime_job(
                    job_spec=job_spec,
                    quote_source=_AssertingQuoteSource(source_id=run_spec.expected_quote_source_id),
                    portfolio_source=alt_source,
                    **common_kwargs,
                )

        with self.subTest(case="portfolio_source_not_pinned_type"):
            with self.assertRaises(PromotedOperationalRuntimeError):
                run_promoted_operational_runtime_job(
                    job_spec=job_spec,
                    quote_source=_AssertingQuoteSource(source_id=run_spec.expected_quote_source_id),
                    portfolio_source=_runner_tests._happy_portfolio_source(),
                    **common_kwargs,
                )

    def test_tampered_run_spec_raises_before_any_source_preflight_clock_or_store_access(
        self,
    ) -> None:
        (
            _,
            run_spec,
            _,
            _,
            portfolio_source,
            _,
            job_spec,
            _,
            _,
        ) = self._fixture()

        object.__setattr__(run_spec, "spec_id", "0" * 64)

        def _explode(*_args, **_kwargs):
            raise AssertionError("store must not be touched before run_spec verification")

        self.advisory_outbox.get = _explode
        self.advisory_outbox.put = _explode
        self.terminal_store.get_optional = _explode
        self.terminal_store.put = _explode

        try:
            run_promoted_operational_runtime_job(
                job_spec=job_spec,
                run_spec=run_spec,
                quote_source=_IdentityExplodingQuoteSource(),
                portfolio_source=portfolio_source,
                clock=_asserting_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_writer=_AssertingBindingBackend(),
                binding_reader=_AssertingBindingBackend(),
                binding_preflight=_AssertingPreflight(),
            )
            self.fail("expected PromotedOperationalRuntimeError")
        except PromotedOperationalRuntimeError as exc:
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)

    def test_wrong_local_dependency_types_fail_before_quote_source_identity_is_read(
        self,
    ) -> None:
        (
            _,
            run_spec,
            _,
            _,
            portfolio_source,
            clock,
            job_spec,
            backend,
            preflight,
        ) = self._fixture()

        cases = {
            "clock_not_callable": {"clock": "not-callable"},
            "advisory_outbox_wrong_type": {"advisory_outbox": object()},
            "terminal_store_wrong_type": {"terminal_store": object()},
            "paper_ledger_wrong_type": {"paper_ledger": object()},
        }
        for case, override in cases.items():
            with self.subTest(case=case):
                kwargs = dict(
                    job_spec=job_spec,
                    run_spec=run_spec,
                    quote_source=_IdentityExplodingQuoteSource(),
                    portfolio_source=portfolio_source,
                    clock=clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    binding_writer=backend,
                    binding_reader=backend,
                    binding_preflight=preflight,
                )
                kwargs.update(override)
                with self.assertRaises(PromotedOperationalRuntimeError):
                    run_promoted_operational_runtime_job(**kwargs)

    def test_valid_fresh_run_seals_once_and_returns_cross_checked_state(self) -> None:
        (
            _,
            run_spec,
            quote_source,
            _,
            portfolio_source,
            clock,
            job_spec,
            backend,
            preflight,
        ) = self._fixture()

        call_order: list[str] = []
        original_terminal_put = self.terminal_store.put

        def _tracking_terminal_put(value):
            result = original_terminal_put(value)
            call_order.append("terminal_put")
            return result

        self.terminal_store.put = _tracking_terminal_put
        original_create_or_verify = backend.create_or_verify

        def _tracking_create_or_verify(**kwargs):
            result = original_create_or_verify(**kwargs)
            call_order.append("seal_write")
            return result

        backend.create_or_verify = _tracking_create_or_verify

        state = run_promoted_operational_runtime_job(
            job_spec=job_spec,
            run_spec=run_spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=preflight,
        )
        self.assertIs(type(state), PromotedOperationalRuntimeState)
        self.assertFalse(state.anchored.reused_existing_terminal)
        self.assertEqual(call_order, ["terminal_put", "seal_write"])
        self.assertEqual(len(backend.writer_calls), 1)
        self.assertEqual(preflight.calls, ["test-bucket"])
        self.assertEqual(state.job_spec, job_spec)
        self.assertEqual(state.anchored.published.terminal.spec_id, job_spec.operational_run_spec_id)

        with self.assertRaises(PromotedOperationalRuntimeError):
            PromotedOperationalRuntimeState(
                job_spec=_dc_replace(job_spec, binding_bucket="a-different-bucket"),
                anchored=state.anchored,
            )

        object.__setattr__(state.anchored, "published", object())
        try:
            PromotedOperationalRuntimeState(job_spec=job_spec, anchored=state.anchored)
            self.fail("expected PromotedOperationalRuntimeError")
        except PromotedOperationalRuntimeError as exc:
            self.assertEqual(str(exc), "promoted operational runtime call is invalid")
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)

    def test_valid_restart_replays_without_quote_or_portfolio_acquisition_or_clock_access(
        self,
    ) -> None:
        (
            _,
            run_spec,
            quote_source,
            portfolio_context,
            portfolio_source,
            clock,
            job_spec,
            backend,
            preflight,
        ) = self._fixture()

        first = run_promoted_operational_runtime_job(
            job_spec=job_spec,
            run_spec=run_spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=preflight,
        )
        self.assertFalse(first.anchored.reused_existing_terminal)
        self.assertEqual(len(backend.writer_calls), 1)

        def _explode(_keys=None):
            raise AssertionError("quote acquisition must not happen on replay")

        exploding_quote_source = _runner_tests._FakeQuoteSource(
            responder=_explode, source_id=run_spec.expected_quote_source_id
        )

        replay_portfolio_source = PinnedPromotedOperationalPortfolioSource(portfolio_context)
        replay_portfolio_source.read_portfolio_context = lambda: (_ for _ in ()).throw(
            AssertionError("portfolio acquisition must not happen on replay")
        )

        second = run_promoted_operational_runtime_job(
            job_spec=job_spec,
            run_spec=run_spec,
            quote_source=exploding_quote_source,
            portfolio_source=replay_portfolio_source,
            clock=_asserting_clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=preflight,
        )
        self.assertTrue(second.anchored.reused_existing_terminal)
        self.assertEqual(second.anchored.published.terminal, first.anchored.published.terminal)
        self.assertEqual(len(backend.writer_calls), 1)
        self.assertEqual(exploding_quote_source.calls, [])

    def test_module_has_no_environment_credential_network_broker_subprocess_or_discovery_capability(
        self,
    ) -> None:
        source = inspect.getsource(_runtime_module)
        for forbidden in (
            "import os",
            "os.environ",
            "os.getenv",
            "import google",
            "google.cloud.storage.Client",
            "import requests",
            "import urllib",
            "import http",
            "import socket",
            "telegram",
            "kiteconnect",
            "import subprocess",
            "datetime.now(",
            "datetime.utcnow(",
            ".utcnow(",
            ".today(",
            "time.time(",
            "import random",
            "glob(",
            "iterdir(",
            "listdir(",
            "rglob(",
            "place_order",
            "modify_order",
            "cancel_order",
        ):
            self.assertNotIn(forbidden, source)
        ast.parse(source)


if __name__ == "__main__":
    unittest.main()
