from __future__ import annotations

import hashlib
import inspect
import tempfile
import unittest
from dataclasses import replace as _dc_replace
from datetime import timedelta
from pathlib import Path

from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_anchored_session import (
    AnchoredPromotedOperationalSessionState,
    PromotedOperationalAnchoredSessionError,
    PromotedOperationalBindingUnreadableError,
    PromotedOperationalControlPlaneUnreachableError,
    PromotedOperationalManualRecoveryRequiredError,
    run_publish_and_anchor_promoted_operational_session,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    build_promoted_operational_advisory,
    build_promoted_operational_terminal_record,
    promoted_paper_registration_from_result,
)
from india_swing.promoted_operational_service import publish_promoted_operational_result
from india_swing.promoted_terminal_binding import (
    build_promoted_operational_terminal_binding_record,
    encode_promoted_operational_terminal_binding_record,
    promoted_operational_terminal_binding_object_name,
)
from india_swing.promoted_terminal_binding_control_plane import ObservedTerminalBindingObject

from tests import test_promoted_operational_persistence as _persistence_tests
from tests import test_promoted_operational_runner as _runner_tests


def _exploding_sources():
    def _explode(_keys=None):
        raise AssertionError("sources must not be touched for this call")

    def _explode_clock():
        raise AssertionError("clock must not be touched for this call")

    return (
        _runner_tests._FakeQuoteSource(responder=_explode),
        _runner_tests._FakePortfolioSource(responder=_explode),
        _explode_clock,
    )


class _PermissiveRecordingPreflight:
    """A permissive fake PromotedTerminalBindingControlPlanePreflight:
    always succeeds and records every call. Used by every test that is not
    specifically exercising preflight failure."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def verify_bucket_reachable(self, *, bucket: str) -> None:
        self.calls.append(bucket)


class _FakeBindingBackend:
    """A single in-memory create-once GCS-like store, exposed as both a
    StateObjectWriter (create_or_verify) and a
    PromotedTerminalBindingObjectReader (read_current/read_current_optional).
    Never contacts GCP."""

    def __init__(self, *, seal_should_fail: bool = False) -> None:
        self.objects: dict[str, bytes] = {}
        self.generations: dict[str, int] = {}
        self.writer_calls: list[dict[str, object]] = []
        self.reader_calls: list[tuple[str, str]] = []
        self._next_generation = 1
        self._seal_should_fail = seal_should_fail

    def create_or_verify(
        self, *, bucket, object_name, content_bytes, content_type, maximum_bytes
    ) -> PublishedStateObject:
        if self._seal_should_fail:
            raise RuntimeError("SECRET-SEAL-FAILURE-DO-NOT-LEAK-4d7f")
        self.writer_calls.append(
            dict(
                bucket=bucket,
                object_name=object_name,
                content_bytes=content_bytes,
                content_type=content_type,
                maximum_bytes=maximum_bytes,
            )
        )
        existing = self.objects.get(object_name)
        if existing is not None:
            if existing != content_bytes:
                raise RuntimeError("SECRET-BINDING-CONFLICT-DO-NOT-LEAK-9a1c")
            return PublishedStateObject(
                object_name=object_name,
                generation=self.generations[object_name],
                byte_count=len(existing),
                sha256=hashlib.sha256(existing).hexdigest(),
            )
        generation = self._next_generation
        self._next_generation += 1
        self.objects[object_name] = content_bytes
        self.generations[object_name] = generation
        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=hashlib.sha256(content_bytes).hexdigest(),
        )

    def _lookup(self, object_name: str) -> ObservedTerminalBindingObject | None:
        content_bytes = self.objects.get(object_name)
        if content_bytes is None:
            return None
        generation = self.generations[object_name]
        return ObservedTerminalBindingObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=hashlib.sha256(content_bytes).hexdigest(),
            content_bytes=content_bytes,
        )

    def read_current(self, *, bucket, object_name, maximum_bytes) -> ObservedTerminalBindingObject:
        self.reader_calls.append(("read_current", object_name))
        observed = self._lookup(object_name)
        if observed is None:
            raise LookupError("not found")
        return observed

    def read_current_optional(
        self, *, bucket, object_name, maximum_bytes
    ) -> ObservedTerminalBindingObject | None:
        self.reader_calls.append(("read_current_optional", object_name))
        return self._lookup(object_name)


class PromotedOperationalAnchoredSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        root = Path(self._temp.name)
        self.advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(root)
        self.terminal_store = LocalPromotedOperationalTerminalStore(root / "runner")
        self.paper_ledger = LocalPaperTradeLedger(root / "paper")

    def test_fresh_session_runs_once_publishes_terminal_last_then_seals_the_binding_exactly_once(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        backend = _FakeBindingBackend()
        preflight = _PermissiveRecordingPreflight()

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

        result = run_publish_and_anchor_promoted_operational_session(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_bucket="test-bucket",
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=preflight,
        )
        self.assertIs(type(result), AnchoredPromotedOperationalSessionState)
        self.assertFalse(result.reused_existing_terminal)
        self.assertEqual(call_order, ["terminal_put", "seal_write"])
        self.assertEqual(len(backend.writer_calls), 1)
        object_name = promoted_operational_terminal_binding_object_name(spec)
        self.assertEqual(backend.writer_calls[0]["object_name"], object_name)
        self.assertEqual(result.binding_object_name, object_name)
        self.assertEqual(result.binding_record.expected_terminal_id, result.published.terminal.terminal_id)
        self.assertEqual(result.binding_record.spec_id, spec.spec_id)
        self.assertEqual(result.binding_generation, backend.generations[object_name])
        self.assertEqual(preflight.calls, ["test-bucket"])

    def test_restart_with_sealed_binding_replays_without_clock_source_or_reseal(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        backend = _FakeBindingBackend()
        preflight = _PermissiveRecordingPreflight()

        first = run_publish_and_anchor_promoted_operational_session(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_bucket="test-bucket",
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=preflight,
        )
        self.assertFalse(first.reused_existing_terminal)
        self.assertEqual(len(backend.writer_calls), 1)

        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        second = run_publish_and_anchor_promoted_operational_session(
            spec=spec,
            quote_source=exploding_quote_source,
            portfolio_source=exploding_portfolio_source,
            clock=exploding_clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_bucket="test-bucket",
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=preflight,
        )
        self.assertTrue(second.reused_existing_terminal)
        self.assertEqual(second.published.terminal, first.published.terminal)
        self.assertEqual(second.published.advisory, first.published.advisory)
        self.assertEqual(second.binding_record.binding_id, first.binding_record.binding_id)
        self.assertEqual(len(backend.writer_calls), 1)
        self.assertEqual(exploding_quote_source.source_id_reads, 0)
        self.assertEqual(exploding_portfolio_source.source_id_reads, 0)
        self.assertEqual(exploding_quote_source.calls, [])
        self.assertEqual(exploding_portfolio_source.calls, 0)

    def test_local_terminal_without_a_sealed_binding_fails_closed_without_rerunning_the_pipeline(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        backend = _FakeBindingBackend()
        preflight = _PermissiveRecordingPreflight()
        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalAnchoredSessionError):
            run_publish_and_anchor_promoted_operational_session(
                spec=result.spec,
                quote_source=exploding_quote_source,
                portfolio_source=exploding_portfolio_source,
                clock=exploding_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=preflight,
            )
        self.assertEqual(exploding_quote_source.calls, [])
        self.assertEqual(exploding_portfolio_source.calls, 0)
        self.assertEqual(len(backend.writer_calls), 0)

    def test_sealed_binding_without_a_local_terminal_fails_closed_before_any_source_or_clock_access(
        self,
    ) -> None:
        other_result = _persistence_tests._complete_paper_buy_result()
        other_advisory = build_promoted_operational_advisory(other_result)
        other_registration = promoted_paper_registration_from_result(other_result, other_advisory)
        other_terminal = build_promoted_operational_terminal_record(
            other_result, other_advisory, other_registration
        )
        backend = _FakeBindingBackend()
        preflight = _PermissiveRecordingPreflight()
        record = build_promoted_operational_terminal_binding_record(other_terminal, other_result.spec)
        payload = encode_promoted_operational_terminal_binding_record(record)
        object_name = promoted_operational_terminal_binding_object_name(other_result.spec)
        backend.objects[object_name] = payload
        backend.generations[object_name] = 1

        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalAnchoredSessionError):
            run_publish_and_anchor_promoted_operational_session(
                spec=other_result.spec,
                quote_source=exploding_quote_source,
                portfolio_source=exploding_portfolio_source,
                clock=exploding_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=preflight,
            )
        self.assertEqual(exploding_quote_source.calls, [])
        self.assertEqual(exploding_portfolio_source.calls, 0)

    def test_unreadable_or_corrupt_binding_is_never_treated_as_absence_and_blocks_the_fresh_run(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        preflight = _PermissiveRecordingPreflight()

        class _RaisingReader:
            def read_current(self, **_kwargs):
                raise AssertionError("read_current must not be called by anchored orchestration")

            def read_current_optional(self, **_kwargs):
                raise RuntimeError("SECRET-CORRUPT-BINDING-DO-NOT-LEAK-8b3e")

        backend_writer = _FakeBindingBackend()
        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        try:
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=exploding_quote_source,
                portfolio_source=exploding_portfolio_source,
                clock=exploding_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend_writer,
                binding_reader=_RaisingReader(),
                binding_preflight=preflight,
            )
            self.fail("expected PromotedOperationalAnchoredSessionError")
        except PromotedOperationalAnchoredSessionError as exc:
            self.assertNotIn("SECRET-CORRUPT-BINDING-DO-NOT-LEAK-8b3e", str(exc))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)
        self.assertEqual(exploding_quote_source.calls, [])
        self.assertEqual(exploding_portfolio_source.calls, 0)
        self.assertIsNone(self.terminal_store.get_optional(spec.spec_id))
        self.assertEqual(len(backend_writer.writer_calls), 0)

    def test_seal_failure_after_a_fresh_publish_fails_closed_without_rollback_and_stays_manual_on_retry(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        backend = _FakeBindingBackend(seal_should_fail=True)
        preflight = _PermissiveRecordingPreflight()

        with self.assertRaises(PromotedOperationalAnchoredSessionError):
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=preflight,
            )

        stored_terminal = self.terminal_store.get(spec.spec_id)
        stored_terminal.verify_content_identity()
        self.advisory_outbox.get(stored_terminal.advisory_id)
        if stored_terminal.paper_registration_id is not None:
            self.paper_ledger.get_registration(stored_terminal.paper_registration_id)
        self.assertEqual(len(backend.objects), 0)

        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalAnchoredSessionError):
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=exploding_quote_source,
                portfolio_source=exploding_portfolio_source,
                clock=exploding_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=preflight,
            )
        self.assertEqual(exploding_quote_source.calls, [])
        self.assertEqual(exploding_portfolio_source.calls, 0)
        self.assertEqual(self.terminal_store.get(spec.spec_id).terminal_id, stored_terminal.terminal_id)
        self.assertEqual(len(backend.objects), 0)

    def test_seal_conflict_against_a_different_previously_sealed_terminal_fails_closed(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        object_name = promoted_operational_terminal_binding_object_name(spec)
        preflight = _PermissiveRecordingPreflight()

        priming_quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        priming_portfolio_source = _runner_tests._happy_portfolio_source()
        priming_result = _persistence_tests._run(spec, priming_quote_source, priming_portfolio_source)
        priming_advisory = build_promoted_operational_advisory(priming_result)
        priming_registration = promoted_paper_registration_from_result(priming_result, priming_advisory)
        priming_terminal = build_promoted_operational_terminal_record(
            priming_result, priming_advisory, priming_registration
        )
        conflicting_terminal = _dc_replace(
            priming_terminal, evaluated_at=priming_terminal.evaluated_at - timedelta(seconds=1)
        )
        self.assertEqual(conflicting_terminal.spec_id, spec.spec_id)
        self.assertNotEqual(conflicting_terminal.terminal_id, priming_terminal.terminal_id)
        conflicting_record = build_promoted_operational_terminal_binding_record(
            conflicting_terminal, spec
        )
        conflicting_payload = encode_promoted_operational_terminal_binding_record(conflicting_record)

        backend = _FakeBindingBackend()
        backend.objects[object_name] = conflicting_payload
        backend.generations[object_name] = 555
        # Simulate a race: the load path's read still proves absence (as
        # if it ran before the concurrent seal became visible), but the
        # seal write itself discovers the conflicting object.
        backend.read_current_optional = lambda *, bucket, object_name, maximum_bytes: None

        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        with self.assertRaises(PromotedOperationalAnchoredSessionError):
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=preflight,
            )
        self.assertEqual(backend.objects[object_name], conflicting_payload)

    def test_anchored_result_rejects_a_binding_record_that_disagrees_with_the_published_terminal_on_redundant_audit_fields(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        backend = _FakeBindingBackend()
        preflight = _PermissiveRecordingPreflight()

        result = run_publish_and_anchor_promoted_operational_session(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_bucket="test-bucket",
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=preflight,
        )

        # The accepted fresh-run result stays accepted: this proves the new
        # cross-check does not weaken any existing valid construction.
        reaccepted = AnchoredPromotedOperationalSessionState(
            published=result.published,
            binding_record=result.binding_record,
            binding_bucket=result.binding_bucket,
            binding_object_name=result.binding_object_name,
            binding_generation=result.binding_generation,
            reused_existing_terminal=result.reused_existing_terminal,
        )
        self.assertEqual(reaccepted, result)

        def _flip_hex(value: str) -> str:
            replacement = "1" if value[0] == "0" else "0"
            return replacement + value[1:]

        with self.subTest(case="terminal_completed_at_mismatch"):
            mismatched_record = _dc_replace(
                result.binding_record,
                terminal_completed_at=(
                    result.binding_record.terminal_completed_at + timedelta(seconds=1)
                ),
            )
            mismatched_record.verify_content_identity()
            self.assertEqual(mismatched_record.spec_id, result.published.terminal.spec_id)
            self.assertEqual(
                mismatched_record.expected_terminal_id, result.published.terminal.terminal_id
            )
            self.assertNotEqual(
                mismatched_record.terminal_completed_at, result.published.terminal.completed_at
            )
            with self.assertRaises(PromotedOperationalAnchoredSessionError):
                AnchoredPromotedOperationalSessionState(
                    published=result.published,
                    binding_record=mismatched_record,
                    binding_bucket=result.binding_bucket,
                    binding_object_name=result.binding_object_name,
                    binding_generation=result.binding_generation,
                    reused_existing_terminal=result.reused_existing_terminal,
                )

        with self.subTest(case="target_session_mismatch"):
            mismatched_record = _dc_replace(
                result.binding_record,
                target_session=result.binding_record.target_session + timedelta(days=1),
            )
            mismatched_record.verify_content_identity()
            self.assertNotEqual(
                mismatched_record.target_session, result.published.terminal.target_session
            )
            with self.assertRaises(PromotedOperationalAnchoredSessionError):
                AnchoredPromotedOperationalSessionState(
                    published=result.published,
                    binding_record=mismatched_record,
                    binding_bucket=result.binding_bucket,
                    binding_object_name=result.binding_object_name,
                    binding_generation=result.binding_generation,
                    reused_existing_terminal=result.reused_existing_terminal,
                )

        with self.subTest(case="preparation_id_mismatch"):
            mismatched_record = _dc_replace(
                result.binding_record,
                preparation_id=_flip_hex(result.binding_record.preparation_id),
            )
            mismatched_record.verify_content_identity()
            self.assertNotEqual(
                mismatched_record.preparation_id, result.published.terminal.preparation_id
            )
            with self.assertRaises(PromotedOperationalAnchoredSessionError):
                AnchoredPromotedOperationalSessionState(
                    published=result.published,
                    binding_record=mismatched_record,
                    binding_bucket=result.binding_bucket,
                    binding_object_name=result.binding_object_name,
                    binding_generation=result.binding_generation,
                    reused_existing_terminal=result.reused_existing_terminal,
                )

    def test_preflight_runs_before_the_binding_load_and_before_any_source_clock_or_local_write(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        call_order: list[str] = []

        class _OrderingPreflight:
            def verify_bucket_reachable(self, *, bucket: str) -> None:
                call_order.append("preflight")

        backend = _FakeBindingBackend()
        original_read_current_optional = backend.read_current_optional

        def _tracking_read_current_optional(**kwargs):
            call_order.append("binding_load")
            return original_read_current_optional(**kwargs)

        backend.read_current_optional = _tracking_read_current_optional

        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        run_publish_and_anchor_promoted_operational_session(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_bucket="test-bucket",
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=_OrderingPreflight(),
        )
        self.assertEqual(call_order, ["preflight", "binding_load"])

        class _FailingPreflight:
            def verify_bucket_reachable(self, *, bucket: str) -> None:
                raise RuntimeError("SECRET-PREFLIGHT-ORDER-DO-NOT-LEAK")

        class _AssertingReader:
            def read_current(self, **_kwargs):
                raise AssertionError("read_current must not be called when preflight fails")

            def read_current_optional(self, **_kwargs):
                raise AssertionError(
                    "read_current_optional must not be called when preflight fails"
                )

        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalControlPlaneUnreachableError):
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=exploding_quote_source,
                portfolio_source=exploding_portfolio_source,
                clock=exploding_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=_AssertingReader(),
                binding_preflight=_FailingPreflight(),
            )
        self.assertEqual(exploding_quote_source.calls, [])
        self.assertEqual(exploding_portfolio_source.calls, 0)

    def test_unreachable_bucket_fails_closed_before_any_market_run_or_local_publication(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)

        class _FailingPreflight:
            def verify_bucket_reachable(self, *, bucket: str) -> None:
                raise RuntimeError("SECRET-UNREACHABLE-DO-NOT-LEAK-6e2a")

        backend = _FakeBindingBackend()
        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalControlPlaneUnreachableError):
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=exploding_quote_source,
                portfolio_source=exploding_portfolio_source,
                clock=exploding_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=_FailingPreflight(),
            )
        self.assertEqual(exploding_quote_source.calls, [])
        self.assertEqual(exploding_portfolio_source.calls, 0)
        self.assertIsNone(self.terminal_store.get_optional(spec.spec_id))
        self.assertEqual(len(backend.writer_calls), 0)
        self.assertEqual(len(backend.reader_calls), 0)

    def test_seal_failure_after_publish_raises_the_typed_manual_recovery_error_without_rollback(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        backend = _FakeBindingBackend(seal_should_fail=True)
        preflight = _PermissiveRecordingPreflight()

        with self.assertRaises(PromotedOperationalManualRecoveryRequiredError) as ctx:
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=preflight,
            )
        self.assertIsInstance(ctx.exception, PromotedOperationalAnchoredSessionError)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

        stored_terminal = self.terminal_store.get(spec.spec_id)
        stored_terminal.verify_content_identity()
        self.advisory_outbox.get(stored_terminal.advisory_id)
        if stored_terminal.paper_registration_id is not None:
            self.paper_ledger.get_registration(stored_terminal.paper_registration_id)
        self.assertEqual(len(backend.objects), 0)

        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalAnchoredSessionError):
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=exploding_quote_source,
                portfolio_source=exploding_portfolio_source,
                clock=exploding_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=preflight,
            )
        self.assertEqual(exploding_quote_source.calls, [])
        self.assertEqual(exploding_portfolio_source.calls, 0)
        self.assertEqual(self.terminal_store.get(spec.spec_id).terminal_id, stored_terminal.terminal_id)
        self.assertEqual(len(backend.objects), 0)

    def test_unreadable_binding_raises_the_typed_control_plane_error_and_never_the_manual_recovery_error(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        preflight = _PermissiveRecordingPreflight()

        class _RaisingReader:
            def read_current(self, **_kwargs):
                raise AssertionError("read_current must not be called")

            def read_current_optional(self, **_kwargs):
                raise RuntimeError("SECRET-CORRUPT-TYPED-DO-NOT-LEAK-1d4f")

        backend_writer = _FakeBindingBackend()
        exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
        try:
            run_publish_and_anchor_promoted_operational_session(
                spec=spec,
                quote_source=exploding_quote_source,
                portfolio_source=exploding_portfolio_source,
                clock=exploding_clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                binding_bucket="test-bucket",
                binding_writer=backend_writer,
                binding_reader=_RaisingReader(),
                binding_preflight=preflight,
            )
            self.fail("expected PromotedOperationalBindingUnreadableError")
        except PromotedOperationalBindingUnreadableError as exc:
            self.assertIsInstance(exc, PromotedOperationalAnchoredSessionError)
            self.assertNotIsInstance(exc, PromotedOperationalManualRecoveryRequiredError)
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)

        # A proven-absent binding still routes normally (fresh run succeeds).
        backend = _FakeBindingBackend()
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
        )
        result = run_publish_and_anchor_promoted_operational_session(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            binding_bucket="test-bucket",
            binding_writer=backend,
            binding_reader=backend,
            binding_preflight=preflight,
        )
        self.assertFalse(result.reused_existing_terminal)

    def test_typed_errors_are_sanitized_subclasses_that_leak_no_bucket_object_name_or_nested_text(
        self,
    ) -> None:
        self.assertTrue(
            issubclass(
                PromotedOperationalControlPlaneUnreachableError, PromotedOperationalAnchoredSessionError
            )
        )
        self.assertTrue(
            issubclass(
                PromotedOperationalBindingUnreadableError, PromotedOperationalAnchoredSessionError
            )
        )
        self.assertTrue(
            issubclass(
                PromotedOperationalManualRecoveryRequiredError, PromotedOperationalAnchoredSessionError
            )
        )

        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        object_name = promoted_operational_terminal_binding_object_name(spec)
        preflight = _PermissiveRecordingPreflight()

        # A service-raised failure stays the plain base type -- never any
        # of the three typed subclasses.
        with self.subTest(case="service_raised_failure_stays_base_type"):
            # A deliberately distinct spec (different chunk_size), so this
            # subtest's local terminal write never collides with the
            # shared `spec` object used by the later subtests below.
            other_preparation, other_spec = _runner_tests._run_spec(chunk_size=1)
            self.assertNotEqual(other_spec.spec_id, spec.spec_id)
            other_quote_source = _runner_tests._FakeQuoteSource(
                responder=_runner_tests._sorted_quote_responder(other_preparation)
            )
            base_result = _persistence_tests._run(
                other_spec, other_quote_source, _runner_tests._happy_portfolio_source()
            )
            publish_promoted_operational_result(
                result=base_result,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
            )
            backend = _FakeBindingBackend()
            exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
            try:
                run_publish_and_anchor_promoted_operational_session(
                    spec=base_result.spec,
                    quote_source=exploding_quote_source,
                    portfolio_source=exploding_portfolio_source,
                    clock=exploding_clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    binding_bucket="test-bucket",
                    binding_writer=backend,
                    binding_reader=backend,
                    binding_preflight=preflight,
                )
                self.fail("expected PromotedOperationalAnchoredSessionError")
            except PromotedOperationalAnchoredSessionError as exc:
                self.assertIs(type(exc), PromotedOperationalAnchoredSessionError)
                self.assertNotIsInstance(exc, PromotedOperationalControlPlaneUnreachableError)
                self.assertNotIsInstance(exc, PromotedOperationalBindingUnreadableError)
                self.assertNotIsInstance(exc, PromotedOperationalManualRecoveryRequiredError)

        with self.subTest(case="unreachable_control_plane_sanitized"):

            class _FailingPreflight:
                def verify_bucket_reachable(self, *, bucket: str) -> None:
                    raise RuntimeError("SECRET-PREFLIGHT-9f1a-" + bucket)

            backend = _FakeBindingBackend()
            exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
            try:
                run_publish_and_anchor_promoted_operational_session(
                    spec=spec,
                    quote_source=exploding_quote_source,
                    portfolio_source=exploding_portfolio_source,
                    clock=exploding_clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    binding_bucket="test-bucket",
                    binding_writer=backend,
                    binding_reader=backend,
                    binding_preflight=_FailingPreflight(),
                )
                self.fail("expected PromotedOperationalControlPlaneUnreachableError")
            except PromotedOperationalControlPlaneUnreachableError as exc:
                self.assertIsInstance(exc, PromotedOperationalAnchoredSessionError)
                self.assertNotIn("SECRET-PREFLIGHT-9f1a", str(exc))
                self.assertNotIn("test-bucket", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="unreadable_binding_sanitized"):

            class _RaisingReader:
                def read_current(self, **_kwargs):
                    raise AssertionError("must not be called")

                def read_current_optional(self, **_kwargs):
                    raise RuntimeError("SECRET-BINDING-READ-2b7c-" + object_name)

            backend = _FakeBindingBackend()
            exploding_quote_source, exploding_portfolio_source, exploding_clock = _exploding_sources()
            try:
                run_publish_and_anchor_promoted_operational_session(
                    spec=spec,
                    quote_source=exploding_quote_source,
                    portfolio_source=exploding_portfolio_source,
                    clock=exploding_clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    binding_bucket="test-bucket",
                    binding_writer=backend,
                    binding_reader=_RaisingReader(),
                    binding_preflight=preflight,
                )
                self.fail("expected PromotedOperationalBindingUnreadableError")
            except PromotedOperationalBindingUnreadableError as exc:
                self.assertIsInstance(exc, PromotedOperationalAnchoredSessionError)
                self.assertNotIn("SECRET-BINDING-READ-2b7c", str(exc))
                self.assertNotIn(object_name, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="manual_recovery_sanitized"):
            seal_fail_backend = _FakeBindingBackend(seal_should_fail=True)
            quote_source = _runner_tests._FakeQuoteSource(
                responder=_runner_tests._sorted_quote_responder(preparation)
            )
            portfolio_source = _runner_tests._happy_portfolio_source()
            clock = _runner_tests._clock(
                _runner_tests._STARTED_AT,
                _runner_tests._RUN_EVALUATED_AT,
                _runner_tests._COMPLETED_AT,
            )
            try:
                run_publish_and_anchor_promoted_operational_session(
                    spec=spec,
                    quote_source=quote_source,
                    portfolio_source=portfolio_source,
                    clock=clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    binding_bucket="test-bucket",
                    binding_writer=seal_fail_backend,
                    binding_reader=seal_fail_backend,
                    binding_preflight=preflight,
                )
                self.fail("expected PromotedOperationalManualRecoveryRequiredError")
            except PromotedOperationalManualRecoveryRequiredError as exc:
                self.assertIsInstance(exc, PromotedOperationalAnchoredSessionError)
                self.assertNotIn("SECRET-SEAL-FAILURE-DO-NOT-LEAK-4d7f", str(exc))
                self.assertNotIn(object_name, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_anchored_session_public_contract_adds_no_environment_credential_network_telegram_broker_or_current_time_capability(
        self,
    ) -> None:
        import india_swing.promoted_operational_anchored_session as module

        source = inspect.getsource(module)
        for forbidden in (
            "import google",
            "google.cloud.storage.Client",
            "import requests",
            "import urllib",
            "import http",
            "import socket",
            "telegram",
            "kiteconnect",
            "import subprocess",
            "os.environ",
            "os.getenv",
            "datetime.now(",
            "datetime.utcnow(",
            ".utcnow(",
            ".today(",
            "time.time(",
            "import random",
            "import pathlib",
            "open(",
        ):
            self.assertNotIn(forbidden, source)

        signature = inspect.signature(run_publish_and_anchor_promoted_operational_session)
        self.assertEqual(
            set(signature.parameters),
            {
                "spec",
                "quote_source",
                "portfolio_source",
                "clock",
                "advisory_outbox",
                "terminal_store",
                "paper_ledger",
                "binding_bucket",
                "binding_writer",
                "binding_reader",
                "binding_preflight",
            },
        )


if __name__ == "__main__":
    unittest.main()
