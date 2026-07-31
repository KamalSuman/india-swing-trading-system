from __future__ import annotations

import inspect
import tempfile
import unittest
from dataclasses import fields as _dc_fields
from dataclasses import replace as _dc_replace
from datetime import timedelta
from pathlib import Path

from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_decision import PromotedOperationalDecisionAction
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    PromotedOperationalPersistenceError,
    PromotedOperationalTerminalRecord,
    build_promoted_operational_advisory,
    encode_promoted_operational_advisory,
    encode_promoted_operational_terminal_record,
    promoted_paper_registration_from_result,
    verify_promoted_operational_published_bundle,
)
from india_swing.promoted_operational_runner import PromotedOperationalRunStatus
from india_swing.promoted_operational_service import (
    PromotedOperationalPublishedState,
    PromotedOperationalServiceError,
    TrustedPromotedOperationalTerminalBinding,
    publish_promoted_operational_result,
    run_and_publish_promoted_operational_service,
)

from tests import test_promoted_operational_persistence as _persistence_tests
from tests import test_promoted_operational_runner as _runner_tests


def _terminal_kwargs(terminal: PromotedOperationalTerminalRecord) -> dict:
    return {
        item.name: getattr(terminal, item.name)
        for item in _dc_fields(terminal)
        if item.name != "terminal_id"
    }


def _flip_hex(value: str) -> str:
    replacement = "1" if value[0] == "0" else "0"
    return replacement + value[1:]


def _exploding_sources():
    def _explode(_keys=None):
        raise AssertionError("sources must not be touched replaying a sealed terminal")

    def _explode_clock():
        raise AssertionError("clock must not be touched replaying a sealed terminal")

    return (
        _runner_tests._FakeQuoteSource(responder=_explode),
        _runner_tests._FakePortfolioSource(responder=_explode),
        _explode_clock,
    )


def _binding_for(spec, terminal) -> TrustedPromotedOperationalTerminalBinding:
    return TrustedPromotedOperationalTerminalBinding(
        spec_id=spec.spec_id, expected_terminal_id=terminal.terminal_id
    )


def _complete_paper_buy_result(*, chunk_size=500):
    """Like ``_persistence_tests._complete_paper_buy_result`` but with a
    caller-chosen chunk size, so tests needing two distinct PAPER_BUY specs
    (and therefore two distinct spec_ids) in the same store don't collide."""

    preparation, spec = _runner_tests._run_spec(chunk_size=chunk_size)
    quote_source = _runner_tests._FakeQuoteSource(
        responder=_runner_tests._sorted_quote_responder(preparation)
    )
    return _persistence_tests._run(spec, quote_source, _runner_tests._happy_portfolio_source())


class PromotedOperationalServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        root = Path(self._temp.name)
        self.advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(root)
        self.terminal_store = LocalPromotedOperationalTerminalStore(root / "runner")
        self.paper_ledger = LocalPaperTradeLedger(root / "paper")

    def test_publish_order_is_advisory_then_registration_then_terminal_and_partial_failure_leaves_no_terminal(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()

        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        self.assertFalse(state.reused_existing_terminal)
        self.assertIsNotNone(state.paper_registration)
        self.advisory_outbox.get(state.advisory.advisory_id)
        self.paper_ledger.get_registration(state.paper_registration.registration_id)
        self.terminal_store.get(result.spec.spec_id)

        # Partial failure: PAPER_BUY publication without a ledger must
        # write neither a paper registration nor a terminal record.
        second_result = _complete_paper_buy_result(chunk_size=1)
        self.assertNotEqual(second_result.spec.spec_id, result.spec.spec_id)
        with self.assertRaises(PromotedOperationalServiceError):
            publish_promoted_operational_result(
                result=second_result,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=None,
            )
        self.assertIsNone(self.terminal_store.get_optional(second_result.spec.spec_id))
        # The advisory itself is safe to have been published first -- it
        # carries no trading authority on its own; only the terminal's
        # absence proves partial-failure ordering was respected.

    def test_retry_after_partial_side_effects_reuses_identical_artifacts_and_seals_terminal_once(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        advisory = self.advisory_outbox.put(build_promoted_operational_advisory(result))
        self.assertIsNone(self.terminal_store.get_optional(result.spec.spec_id))

        first_state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        second_state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        self.assertEqual(first_state.advisory.advisory_id, advisory.advisory_id)
        self.assertEqual(first_state.terminal, second_state.terminal)
        self.assertEqual(first_state.advisory, second_state.advisory)
        self.assertEqual(first_state.paper_registration, second_state.paper_registration)
        self.assertEqual(
            len(list(self.terminal_store.root.glob("*.json"))),
            1,
        )

    def test_existing_terminal_retry_touches_no_clock_source_property_or_acquisition_method_and_returns_equal_artifacts(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock_values = iter(
            (_runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT)
        )
        clock_calls = []

        def clock():
            clock_calls.append(1)
            return next(clock_values)

        first_state = run_and_publish_promoted_operational_service(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        self.assertFalse(first_state.reused_existing_terminal)
        self.assertEqual(len(clock_calls), 3)
        self.assertEqual(quote_source.calls, [("NSE:RELIANCE", "NSE:TCS")])
        self.assertEqual(portfolio_source.calls, 1)

        # The trust anchor is captured from THIS fresh call's returned
        # terminal_id -- exactly as an external control plane would --
        # never by reading terminal_store during the retry under test.
        binding = _binding_for(spec, first_state.terminal)

        def _explode_clock():
            raise AssertionError("clock must not be touched on a sealed-terminal retry")

        def _explode_quotes(_keys):
            raise AssertionError("quote source must not be touched on a sealed-terminal retry")

        def _explode_portfolio():
            raise AssertionError(
                "portfolio source must not be touched on a sealed-terminal retry"
            )

        retry_quote_source = _runner_tests._FakeQuoteSource(responder=_explode_quotes)
        retry_portfolio_source = _runner_tests._FakePortfolioSource(responder=_explode_portfolio)

        second_state = run_and_publish_promoted_operational_service(
            spec=spec,
            quote_source=retry_quote_source,
            portfolio_source=retry_portfolio_source,
            clock=_explode_clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            terminal_binding=binding,
        )
        self.assertTrue(second_state.reused_existing_terminal)
        self.assertEqual(retry_quote_source.source_id_reads, 0)
        self.assertEqual(retry_portfolio_source.source_id_reads, 0)
        self.assertEqual(retry_quote_source.calls, [])
        self.assertEqual(retry_portfolio_source.calls, 0)
        self.assertEqual(second_state.terminal, first_state.terminal)
        self.assertEqual(second_state.advisory, first_state.advisory)
        self.assertEqual(second_state.paper_registration, first_state.paper_registration)

    def test_existing_terminal_with_missing_corrupt_or_cross_linked_side_effect_fails_without_rerun(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        binding = _binding_for(result.spec, state.terminal)

        def _explode(*_args, **_kwargs):
            raise AssertionError("must never re-run the market pipeline for a sealed terminal")

        exploding_quote_source = _runner_tests._FakeQuoteSource(responder=_explode)
        exploding_portfolio_source = _runner_tests._FakePortfolioSource(responder=_explode)

        with self.subTest(case="missing_advisory"):
            advisory_path = self.advisory_outbox.path_for(state.advisory.advisory_id)
            saved = advisory_path.read_bytes()
            advisory_path.unlink()
            with self.assertRaises(Exception):
                run_and_publish_promoted_operational_service(
                    spec=result.spec,
                    quote_source=exploding_quote_source,
                    portfolio_source=exploding_portfolio_source,
                    clock=_explode,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    terminal_binding=binding,
                )
            advisory_path.write_bytes(saved)

        with self.subTest(case="missing_paper_registration"):
            registration_path = self.paper_ledger.registration_path(
                state.paper_registration.registration_id
            )
            saved_registration = registration_path.read_bytes()
            registration_path.unlink()
            with self.assertRaises(PromotedOperationalServiceError):
                run_and_publish_promoted_operational_service(
                    spec=result.spec,
                    quote_source=exploding_quote_source,
                    portfolio_source=exploding_portfolio_source,
                    clock=_explode,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    terminal_binding=binding,
                )
            registration_path.write_bytes(saved_registration)

        with self.subTest(case="no_ledger_supplied_but_terminal_references_one"):
            with self.assertRaises(PromotedOperationalServiceError):
                run_and_publish_promoted_operational_service(
                    spec=result.spec,
                    quote_source=exploding_quote_source,
                    portfolio_source=exploding_portfolio_source,
                    clock=_explode,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=None,
                    terminal_binding=binding,
                )

    def test_failed_and_complete_no_trade_publication_never_require_or_write_paper_registration(
        self,
    ) -> None:
        failed_result = _persistence_tests._failed_result()
        failed_state = publish_promoted_operational_result(
            result=failed_result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=None,
        )
        self.assertIsNone(failed_state.paper_registration)
        self.assertIsNone(failed_state.terminal.paper_registration_id)
        self.assertEqual(failed_state.terminal.status, PromotedOperationalRunStatus.FAILED)

        no_trade_result = _persistence_tests._complete_no_trade_result()
        no_trade_state = publish_promoted_operational_result(
            result=no_trade_result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=None,
        )
        self.assertIsNone(no_trade_state.paper_registration)
        self.assertIsNone(no_trade_state.terminal.paper_registration_id)
        self.assertEqual(
            no_trade_state.terminal.action, PromotedOperationalDecisionAction.NO_TRADE
        )

    def test_nested_mutation_status_action_authority_missing_middle_side_effect_and_self_consistent_id_forgery_fail_closed(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )

        # Direct construction: terminal/advisory cross-link mismatch.
        no_trade_result = _persistence_tests._complete_no_trade_result()
        foreign_advisory = self.advisory_outbox.put(
            build_promoted_operational_advisory(no_trade_result)
        )
        with self.assertRaises(PromotedOperationalServiceError):
            PromotedOperationalPublishedState(
                terminal=state.terminal,
                advisory=foreign_advisory,
                paper_registration=state.paper_registration,
                reused_existing_terminal=False,
            )

        # Missing middle side effect: a paper_registration_id on the terminal
        # with no PaperTradeRegistration supplied.
        with self.assertRaises(PromotedOperationalServiceError):
            PromotedOperationalPublishedState(
                terminal=state.terminal,
                advisory=state.advisory,
                paper_registration=None,
                reused_existing_terminal=False,
            )

        # A registration present when the terminal claims none.
        no_trade_state = publish_promoted_operational_result(
            result=no_trade_result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=None,
        )
        with self.assertRaises(PromotedOperationalServiceError):
            PromotedOperationalPublishedState(
                terminal=no_trade_state.terminal,
                advisory=no_trade_state.advisory,
                paper_registration=state.paper_registration,
                reused_existing_terminal=False,
            )

        # Self-consistent forgery: tamper the stored terminal's action after
        # publication and recompute a self-consistent terminal_id -- still
        # fails structural cross-link/shape validation on the next replay.
        object.__setattr__(state.terminal, "action", PromotedOperationalDecisionAction.NO_TRADE)
        object.__setattr__(state.terminal, "terminal_id", state.terminal._calculated_id())
        with self.assertRaises(Exception):
            state.terminal.verify_content_identity()

    def test_sealed_buy_cannot_be_rewritten_and_replayed_as_no_trade(self) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        self.assertEqual(state.terminal.status, PromotedOperationalRunStatus.COMPLETE)
        self.assertEqual(state.terminal.action, PromotedOperationalDecisionAction.PAPER_BUY)
        # The trust anchor is captured from the ORIGINAL terminal, before
        # any tampering -- exactly as an external control plane would have
        # durably recorded it the moment this fresh run completed.
        binding = _binding_for(result.spec, state.terminal)

        # Attacker bypasses LocalPromotedOperationalTerminalStore.put()'s own
        # create-once conflict check and overwrites the spec_id-keyed file
        # directly with a self-consistent (recomputed terminal_id) FAILED/
        # NO_TRADE forgery -- but still pointing at the ORIGINAL, untouched
        # COMPLETE/PAPER_BUY advisory (same advisory_id/advisory_sha256).
        forged = PromotedOperationalTerminalRecord(
            spec_id=state.terminal.spec_id,
            result_id=state.terminal.result_id,
            target_session=state.terminal.target_session,
            status=PromotedOperationalRunStatus.FAILED,
            action=PromotedOperationalDecisionAction.NO_TRADE,
            started_at=state.terminal.started_at,
            evaluated_at=None,
            completed_at=state.terminal.started_at,
            quote_source_id=None,
            portfolio_source_id=None,
            preparation_id=state.terminal.preparation_id,
            quote_batch_id=None,
            portfolio_context_id=None,
            portfolio_snapshot_id=None,
            quote_gate_batch_id=None,
            allocation_batch_id=None,
            decision_id=None,
            package_id=None,
            advisory_id=state.terminal.advisory_id,
            advisory_sha256=state.terminal.advisory_sha256,
            paper_registration_id=None,
            failure_codes=("SOURCE_IDENTITY_INVALID",),
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )
        self.assertNotEqual(forged.terminal_id, state.terminal.terminal_id)
        self.terminal_store.path_for(state.terminal.spec_id).write_bytes(
            encode_promoted_operational_terminal_record(forged)
        )

        quote_source, portfolio_source, clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalServiceError):
            run_and_publish_promoted_operational_service(
                spec=result.spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                terminal_binding=binding,
            )
        self.assertEqual(quote_source.source_id_reads, 0)
        self.assertEqual(portfolio_source.source_id_reads, 0)
        self.assertEqual(quote_source.calls, [])
        self.assertEqual(portfolio_source.calls, 0)

    def test_coordinated_advisory_and_terminal_rewrite_is_rejected_by_independent_binding_without_any_side_effect_or_source_access(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        # Captured from the ORIGINAL, pre-attack terminal -- the exact
        # value an independent control plane would have durably anchored
        # the moment this run completed.
        original_binding = _binding_for(result.spec, state.terminal)

        # Reproduce Codex's exact coordinated rewrite: both the advisory
        # AND the terminal are rewritten together, self-consistently, so
        # they still agree with each other on action/status/decision/
        # package -- defeating verify_promoted_operational_published_bundle,
        # which only checks that the two LOCAL artifacts agree with each
        # other, not that either one is the artifact that was originally
        # sealed.
        forged_advisory = _dc_replace(
            state.advisory, action=PromotedOperationalDecisionAction.NO_TRADE
        )
        self.assertNotEqual(forged_advisory.advisory_id, state.advisory.advisory_id)
        forged_terminal = _dc_replace(
            state.terminal,
            action=PromotedOperationalDecisionAction.NO_TRADE,
            advisory_id=forged_advisory.advisory_id,
            advisory_sha256=forged_advisory.advisory_sha256,
            paper_registration_id=None,
        )
        self.assertNotEqual(forged_terminal.terminal_id, state.terminal.terminal_id)
        # The forged pair is genuinely internally self-consistent with each
        # other -- proving the bundle verifier alone cannot catch this.
        verify_promoted_operational_published_bundle(forged_terminal, forged_advisory, None)

        self.advisory_outbox.path_for(forged_advisory.advisory_id).write_bytes(
            encode_promoted_operational_advisory(forged_advisory)
        )
        self.terminal_store.path_for(forged_terminal.spec_id).write_bytes(
            encode_promoted_operational_terminal_record(forged_terminal)
        )

        # The original binding's expected_terminal_id no longer matches the
        # rewritten local terminal, so this must be rejected purely on that
        # mismatch -- before advisory_outbox.get, paper_ledger, clock, or
        # any source is ever touched. If the forged advisory were reached
        # (a bug in ordering), it would load and cross-check cleanly
        # against the forged terminal and this call would NOT raise --
        # so a non-raise outcome here is itself proof the ordering broke.
        quote_source, portfolio_source, clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalServiceError):
            run_and_publish_promoted_operational_service(
                spec=result.spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                terminal_binding=original_binding,
            )
        self.assertEqual(quote_source.source_id_reads, 0)
        self.assertEqual(portfolio_source.source_id_reads, 0)
        self.assertEqual(quote_source.calls, [])
        self.assertEqual(portfolio_source.calls, 0)

    def test_existing_terminal_requires_exact_trusted_binding_and_clean_retry_reuses_artifacts(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        correct_binding = _binding_for(result.spec, state.terminal)

        def _assert_rejected(binding) -> None:
            quote_source, portfolio_source, clock = _exploding_sources()
            with self.assertRaises(PromotedOperationalServiceError):
                run_and_publish_promoted_operational_service(
                    spec=result.spec,
                    quote_source=quote_source,
                    portfolio_source=portfolio_source,
                    clock=clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    terminal_binding=binding,
                )
            self.assertEqual(quote_source.source_id_reads, 0)
            self.assertEqual(portfolio_source.source_id_reads, 0)
            self.assertEqual(quote_source.calls, [])
            self.assertEqual(portfolio_source.calls, 0)

        with self.subTest(case="missing_binding"):
            _assert_rejected(None)

        with self.subTest(case="malformed_binding_wrong_type"):
            _assert_rejected("not-a-binding")

        with self.subTest(case="wrong_terminal_id"):
            _assert_rejected(
                TrustedPromotedOperationalTerminalBinding(
                    spec_id=result.spec.spec_id,
                    expected_terminal_id=_flip_hex(state.terminal.terminal_id),
                )
            )

        with self.subTest(case="foreign_spec_id"):
            other_result = _complete_paper_buy_result(chunk_size=1)
            other_state = publish_promoted_operational_result(
                result=other_result,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
            )
            _assert_rejected(_binding_for(other_result.spec, other_state.terminal))

        with self.subTest(case="exact_binding_success"):
            quote_source, portfolio_source, clock = _exploding_sources()
            replayed = run_and_publish_promoted_operational_service(
                spec=result.spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                terminal_binding=correct_binding,
            )
            self.assertTrue(replayed.reused_existing_terminal)
            self.assertEqual(replayed.terminal, state.terminal)
            self.assertEqual(replayed.advisory, state.advisory)
            self.assertEqual(replayed.paper_registration, state.paper_registration)
            self.assertEqual(quote_source.source_id_reads, 0)
            self.assertEqual(portfolio_source.source_id_reads, 0)
            self.assertEqual(quote_source.calls, [])
            self.assertEqual(portfolio_source.calls, 0)

    def test_terminal_absence_with_binding_fails_before_clock_sources_advisory_or_ledger(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        phantom_binding = TrustedPromotedOperationalTerminalBinding(
            spec_id=spec.spec_id, expected_terminal_id="0" * 64
        )
        quote_source, portfolio_source, clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalServiceError):
            run_and_publish_promoted_operational_service(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                terminal_binding=phantom_binding,
            )
        self.assertEqual(quote_source.source_id_reads, 0)
        self.assertEqual(portfolio_source.source_id_reads, 0)
        self.assertEqual(quote_source.calls, [])
        self.assertEqual(portfolio_source.calls, 0)
        self.assertIsNone(self.terminal_store.get_optional(spec.spec_id))

        real_quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        real_portfolio_source = _runner_tests._happy_portfolio_source()
        real_clock = _runner_tests._clock(
            _runner_tests._STARTED_AT,
            _runner_tests._RUN_EVALUATED_AT,
            _runner_tests._COMPLETED_AT,
        )
        fresh = run_and_publish_promoted_operational_service(
            spec=spec,
            quote_source=real_quote_source,
            portfolio_source=real_portfolio_source,
            clock=real_clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
            terminal_binding=None,
        )
        self.assertFalse(fresh.reused_existing_terminal)
        self.assertIsNotNone(fresh.terminal.terminal_id)
        self.assertEqual(
            self.terminal_store.get(spec.spec_id).terminal_id, fresh.terminal.terminal_id
        )

    def test_compact_prefix_rejects_nonempty_complete_without_quote_batch_zero_candidate_with_quote_batch_and_orphan_portfolio_ids(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        base = state.terminal

        with self.subTest(case="orphan_portfolio_context_without_snapshot"):
            with self.assertRaises(PromotedOperationalPersistenceError):
                PromotedOperationalTerminalRecord(
                    spec_id=base.spec_id,
                    result_id=base.result_id,
                    target_session=base.target_session,
                    status=PromotedOperationalRunStatus.FAILED,
                    action=PromotedOperationalDecisionAction.NO_TRADE,
                    started_at=base.started_at,
                    evaluated_at=None,
                    completed_at=base.started_at,
                    quote_source_id=base.quote_source_id,
                    portfolio_source_id=base.portfolio_source_id,
                    preparation_id=base.preparation_id,
                    quote_batch_id=base.quote_batch_id,
                    portfolio_context_id=base.portfolio_context_id,
                    portfolio_snapshot_id=None,
                    quote_gate_batch_id=None,
                    allocation_batch_id=None,
                    decision_id=None,
                    package_id=None,
                    advisory_id=base.advisory_id,
                    advisory_sha256=base.advisory_sha256,
                    paper_registration_id=None,
                    failure_codes=("PORTFOLIO_ACQUISITION_FAILED",),
                    paper_only=True,
                    notification_eligible=False,
                    execution_eligible=False,
                )

        with self.subTest(case="orphan_portfolio_snapshot_without_context"):
            with self.assertRaises(PromotedOperationalPersistenceError):
                PromotedOperationalTerminalRecord(
                    spec_id=base.spec_id,
                    result_id=base.result_id,
                    target_session=base.target_session,
                    status=PromotedOperationalRunStatus.FAILED,
                    action=PromotedOperationalDecisionAction.NO_TRADE,
                    started_at=base.started_at,
                    evaluated_at=None,
                    completed_at=base.started_at,
                    quote_source_id=base.quote_source_id,
                    portfolio_source_id=base.portfolio_source_id,
                    preparation_id=base.preparation_id,
                    quote_batch_id=base.quote_batch_id,
                    portfolio_context_id=None,
                    portfolio_snapshot_id=base.portfolio_snapshot_id,
                    quote_gate_batch_id=None,
                    allocation_batch_id=None,
                    decision_id=None,
                    package_id=None,
                    advisory_id=base.advisory_id,
                    advisory_sha256=base.advisory_sha256,
                    paper_registration_id=None,
                    failure_codes=("PORTFOLIO_ACQUISITION_FAILED",),
                    paper_only=True,
                    notification_eligible=False,
                    execution_eligible=False,
                )

        def _assert_rejected_via_replay(spec, forged, *, ledger) -> None:
            store_path = self.terminal_store.path_for(forged.spec_id)
            store_path.write_bytes(encode_promoted_operational_terminal_record(forged))
            binding = TrustedPromotedOperationalTerminalBinding(
                spec_id=forged.spec_id, expected_terminal_id=forged.terminal_id
            )
            quote_source, portfolio_source, clock = _exploding_sources()
            with self.assertRaises(PromotedOperationalServiceError):
                run_and_publish_promoted_operational_service(
                    spec=spec,
                    quote_source=quote_source,
                    portfolio_source=portfolio_source,
                    clock=clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=ledger,
                    terminal_binding=binding,
                )
            self.assertEqual(quote_source.calls, [])
            self.assertEqual(portfolio_source.calls, 0)

        with self.subTest(case="nonempty_complete_without_quote_batch"):
            forged_missing_quote = PromotedOperationalTerminalRecord(
                **{**_terminal_kwargs(base), "quote_batch_id": None}
            )
            self.assertNotEqual(forged_missing_quote.terminal_id, base.terminal_id)
            _assert_rejected_via_replay(
                result.spec, forged_missing_quote, ledger=self.paper_ledger
            )

        with self.subTest(case="zero_candidate_with_quote_batch"):
            zero_result = _persistence_tests._zero_candidate_complete_result()
            zero_state = publish_promoted_operational_result(
                result=zero_result,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=None,
            )
            forged_zero_with_quote = PromotedOperationalTerminalRecord(
                **{
                    **_terminal_kwargs(zero_state.terminal),
                    "quote_batch_id": base.quote_batch_id,
                }
            )
            self.assertNotEqual(
                forged_zero_with_quote.terminal_id, zero_state.terminal.terminal_id
            )
            _assert_rejected_via_replay(zero_result.spec, forged_zero_with_quote, ledger=None)

    def test_replay_rejects_every_terminal_advisory_semantic_cross_link_mismatch_without_reacquisition(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        terminal_path = self.terminal_store.path_for(state.terminal.spec_id)
        original_bytes = terminal_path.read_bytes()
        base = state.terminal
        # Captured once from the original terminal, before any of the
        # per-subtest tampering below.
        binding = _binding_for(result.spec, base)

        def _assert_forged_rejected(forged: PromotedOperationalTerminalRecord) -> None:
            self.assertNotEqual(forged.terminal_id, base.terminal_id)
            terminal_path.write_bytes(encode_promoted_operational_terminal_record(forged))
            quote_source, portfolio_source, clock = _exploding_sources()
            with self.assertRaises(PromotedOperationalServiceError):
                run_and_publish_promoted_operational_service(
                    spec=result.spec,
                    quote_source=quote_source,
                    portfolio_source=portfolio_source,
                    clock=clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=self.paper_ledger,
                    terminal_binding=binding,
                )
            self.assertEqual(quote_source.calls, [])
            self.assertEqual(portfolio_source.calls, 0)
            terminal_path.write_bytes(original_bytes)

        with self.subTest(case="action_mismatch"):
            _assert_forged_rejected(
                PromotedOperationalTerminalRecord(
                    **{
                        **_terminal_kwargs(base),
                        "action": PromotedOperationalDecisionAction.NO_TRADE,
                        "paper_registration_id": None,
                    }
                )
            )

        with self.subTest(case="evaluated_at_mismatch"):
            _assert_forged_rejected(
                PromotedOperationalTerminalRecord(
                    **{
                        **_terminal_kwargs(base),
                        "evaluated_at": base.evaluated_at - timedelta(seconds=1),
                    }
                )
            )

        with self.subTest(case="decision_id_mismatch"):
            _assert_forged_rejected(
                PromotedOperationalTerminalRecord(
                    **{
                        **_terminal_kwargs(base),
                        "decision_id": _flip_hex(base.decision_id),
                    }
                )
            )

        with self.subTest(case="package_id_mismatch"):
            _assert_forged_rejected(
                PromotedOperationalTerminalRecord(
                    **{
                        **_terminal_kwargs(base),
                        "package_id": _flip_hex(base.package_id),
                    }
                )
            )

        with self.subTest(case="failure_codes_mismatch"):
            failed_result = _persistence_tests._failed_result()
            failed_state = publish_promoted_operational_result(
                result=failed_result,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=None,
            )
            self.assertEqual(failed_state.terminal.failure_codes, ("QUOTE_ACQUISITION_FAILED",))
            failed_binding = _binding_for(failed_result.spec, failed_state.terminal)
            failed_path = self.terminal_store.path_for(failed_state.terminal.spec_id)
            failed_original_bytes = failed_path.read_bytes()
            forged_failed = PromotedOperationalTerminalRecord(
                **{
                    **_terminal_kwargs(failed_state.terminal),
                    "failure_codes": ("QUOTE_COVERAGE_INVALID",),
                }
            )
            self.assertNotEqual(forged_failed.terminal_id, failed_state.terminal.terminal_id)
            failed_path.write_bytes(encode_promoted_operational_terminal_record(forged_failed))
            quote_source, portfolio_source, clock = _exploding_sources()
            with self.assertRaises(PromotedOperationalServiceError):
                run_and_publish_promoted_operational_service(
                    spec=failed_result.spec,
                    quote_source=quote_source,
                    portfolio_source=portfolio_source,
                    clock=clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=None,
                    terminal_binding=failed_binding,
                )
            self.assertEqual(quote_source.calls, [])
            self.assertEqual(portfolio_source.calls, 0)
            failed_path.write_bytes(failed_original_bytes)

    def test_replay_rejects_registration_cross_link_substitution_without_reacquisition(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        binding = _binding_for(result.spec, state.terminal)

        other_result = _complete_paper_buy_result(chunk_size=1)
        self.assertNotEqual(other_result.spec.spec_id, result.spec.spec_id)
        other_advisory = self.advisory_outbox.put(
            build_promoted_operational_advisory(other_result)
        )
        other_registration = promoted_paper_registration_from_result(
            other_result, other_advisory
        )
        stored_other_registration = self.paper_ledger.register_value(other_registration)
        self.assertNotEqual(
            stored_other_registration.registration_id,
            state.paper_registration.registration_id,
        )

        # A genuinely valid, independently-verifiable registration -- for a
        # different spec/result entirely -- substituted in as this
        # terminal's cross-link.
        forged = PromotedOperationalTerminalRecord(
            **{
                **_terminal_kwargs(state.terminal),
                "paper_registration_id": stored_other_registration.registration_id,
            }
        )
        self.assertNotEqual(forged.terminal_id, state.terminal.terminal_id)
        self.terminal_store.path_for(state.terminal.spec_id).write_bytes(
            encode_promoted_operational_terminal_record(forged)
        )

        quote_source, portfolio_source, clock = _exploding_sources()
        with self.assertRaises(PromotedOperationalServiceError):
            run_and_publish_promoted_operational_service(
                spec=result.spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=self.paper_ledger,
                terminal_binding=binding,
            )
        self.assertEqual(quote_source.calls, [])
        self.assertEqual(portfolio_source.calls, 0)

    def test_terminal_spec_binding_rejects_wrong_session_preparation_sources_candidate_depth_and_failure_matrix_without_reacquisition(
        self,
    ) -> None:
        result = _persistence_tests._complete_paper_buy_result()
        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=self.paper_ledger,
        )
        base = state.terminal
        terminal_path = self.terminal_store.path_for(base.spec_id)
        original_bytes = terminal_path.read_bytes()

        def _assert_forged_rejected(spec, forged, *, ledger=self.paper_ledger) -> None:
            store_path = self.terminal_store.path_for(forged.spec_id)
            saved = store_path.read_bytes() if store_path.exists() else None
            store_path.write_bytes(encode_promoted_operational_terminal_record(forged))
            binding = TrustedPromotedOperationalTerminalBinding(
                spec_id=forged.spec_id, expected_terminal_id=forged.terminal_id
            )
            quote_source, portfolio_source, clock = _exploding_sources()
            with self.assertRaises(PromotedOperationalServiceError):
                run_and_publish_promoted_operational_service(
                    spec=spec,
                    quote_source=quote_source,
                    portfolio_source=portfolio_source,
                    clock=clock,
                    advisory_outbox=self.advisory_outbox,
                    terminal_store=self.terminal_store,
                    paper_ledger=ledger,
                    terminal_binding=binding,
                )
            self.assertEqual(quote_source.calls, [])
            self.assertEqual(portfolio_source.calls, 0)
            if saved is not None:
                store_path.write_bytes(saved)

        with self.subTest(case="wrong_session"):
            _assert_forged_rejected(
                result.spec,
                PromotedOperationalTerminalRecord(
                    **{
                        **_terminal_kwargs(base),
                        "target_session": base.target_session + timedelta(days=1),
                    }
                ),
            )
            terminal_path.write_bytes(original_bytes)

        with self.subTest(case="wrong_preparation"):
            _assert_forged_rejected(
                result.spec,
                PromotedOperationalTerminalRecord(
                    **{
                        **_terminal_kwargs(base),
                        "preparation_id": _flip_hex(base.preparation_id),
                    }
                ),
            )
            terminal_path.write_bytes(original_bytes)

        with self.subTest(case="wrong_sources"):
            _assert_forged_rejected(
                result.spec,
                PromotedOperationalTerminalRecord(
                    **{
                        **_terminal_kwargs(base),
                        "quote_source_id": _flip_hex(base.quote_source_id),
                    }
                ),
            )
            terminal_path.write_bytes(original_bytes)

        with self.subTest(case="candidate_depth_zero_candidate_spec"):
            zero_result = _persistence_tests._zero_candidate_complete_result()
            zero_state = publish_promoted_operational_result(
                result=zero_result,
                advisory_outbox=self.advisory_outbox,
                terminal_store=self.terminal_store,
                paper_ledger=None,
            )
            self.assertTrue(bool(base.quote_batch_id))
            forged_zero = PromotedOperationalTerminalRecord(
                spec_id=zero_state.terminal.spec_id,
                result_id=zero_state.terminal.result_id,
                target_session=zero_state.terminal.target_session,
                status=PromotedOperationalRunStatus.FAILED,
                action=PromotedOperationalDecisionAction.NO_TRADE,
                started_at=zero_state.terminal.started_at,
                evaluated_at=None,
                completed_at=zero_state.terminal.started_at,
                quote_source_id=zero_state.terminal.quote_source_id,
                portfolio_source_id=zero_state.terminal.portfolio_source_id,
                preparation_id=zero_state.terminal.preparation_id,
                quote_batch_id=base.quote_batch_id,
                portfolio_context_id=None,
                portfolio_snapshot_id=None,
                quote_gate_batch_id=None,
                allocation_batch_id=None,
                decision_id=None,
                package_id=None,
                advisory_id=zero_state.terminal.advisory_id,
                advisory_sha256=zero_state.terminal.advisory_sha256,
                paper_registration_id=None,
                failure_codes=("PORTFOLIO_ACQUISITION_FAILED",),
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )
            _assert_forged_rejected(zero_result.spec, forged_zero, ledger=None)

        with self.subTest(case="failure_matrix_allocation_failed_wrong_depth"):
            forged_matrix = PromotedOperationalTerminalRecord(
                spec_id=base.spec_id,
                result_id=base.result_id,
                target_session=base.target_session,
                status=PromotedOperationalRunStatus.FAILED,
                action=PromotedOperationalDecisionAction.NO_TRADE,
                started_at=base.started_at,
                evaluated_at=base.evaluated_at,
                completed_at=base.evaluated_at,
                quote_source_id=base.quote_source_id,
                portfolio_source_id=base.portfolio_source_id,
                preparation_id=base.preparation_id,
                quote_batch_id=base.quote_batch_id,
                portfolio_context_id=base.portfolio_context_id,
                portfolio_snapshot_id=base.portfolio_snapshot_id,
                quote_gate_batch_id=None,
                allocation_batch_id=None,
                decision_id=None,
                package_id=None,
                advisory_id=base.advisory_id,
                advisory_sha256=base.advisory_sha256,
                paper_registration_id=None,
                failure_codes=("ALLOCATION_FAILED",),
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )
            _assert_forged_rejected(result.spec, forged_matrix)
            terminal_path.write_bytes(original_bytes)

    def test_clock_non_monotonic_valid_failed_results_publish_and_replay_terminal_last(
        self,
    ) -> None:
        preparation, spec = _runner_tests._run_spec(chunk_size=500)
        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation)
        )
        portfolio_source = _runner_tests._happy_portfolio_source()
        clock = _runner_tests._clock(
            _runner_tests._STARTED_AT,
            _runner_tests._STARTED_AT - timedelta(minutes=5),
            _runner_tests._COMPLETED_AT,
        )
        result = _runner_tests.execute_promoted_operational_run(
            spec=spec, quote_source=quote_source, portfolio_source=portfolio_source, clock=clock
        )
        self.assertIn("CLOCK_NON_MONOTONIC", result.failure_codes)
        self.assertLess(result.evaluated_at, result.started_at)

        state = publish_promoted_operational_result(
            result=result,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=None,
        )
        self.assertEqual(state.terminal.status, PromotedOperationalRunStatus.FAILED)
        self.assertLess(state.terminal.evaluated_at, state.terminal.started_at)
        state.terminal.verify_content_identity()
        binding = _binding_for(spec, state.terminal)

        replay_quote_source, replay_portfolio_source, replay_clock = _exploding_sources()
        replayed = run_and_publish_promoted_operational_service(
            spec=spec,
            quote_source=replay_quote_source,
            portfolio_source=replay_portfolio_source,
            clock=replay_clock,
            advisory_outbox=self.advisory_outbox,
            terminal_store=self.terminal_store,
            paper_ledger=None,
            terminal_binding=binding,
        )
        self.assertTrue(replayed.reused_existing_terminal)
        self.assertEqual(replayed.terminal, state.terminal)
        self.assertEqual(replayed.advisory, state.advisory)
        self.assertEqual(replay_quote_source.source_id_reads, 0)
        self.assertEqual(replay_portfolio_source.source_id_reads, 0)
        self.assertEqual(replay_quote_source.calls, [])
        self.assertEqual(replay_portfolio_source.calls, 0)

    def test_public_contract_has_no_telegram_gcp_environment_network_broker_order_or_current_time_capability(
        self,
    ) -> None:
        import india_swing.promoted_operational_persistence as persistence_module
        import india_swing.promoted_operational_service as service_module

        for module in (persistence_module, service_module):
            source = inspect.getsource(module)
            for forbidden in (
                "import socket",
                "import requests",
                "import kiteconnect",
                "import boto3",
                "import subprocess",
                "google.cloud",
                "telegram",
                "os.environ",
                "os.getenv",
                "datetime.now(",
                "datetime.utcnow(",
                ".utcnow(",
            ):
                self.assertNotIn(forbidden, source)

        service_signature = inspect.signature(run_and_publish_promoted_operational_service)
        self.assertEqual(
            set(service_signature.parameters),
            {
                "spec",
                "quote_source",
                "portfolio_source",
                "clock",
                "advisory_outbox",
                "terminal_store",
                "paper_ledger",
                "terminal_binding",
            },
        )
        publish_signature = inspect.signature(publish_promoted_operational_result)
        self.assertEqual(
            set(publish_signature.parameters),
            {"result", "advisory_outbox", "terminal_store", "paper_ledger"},
        )

        # The typed trust anchor is exact-field, strictly validated, and
        # carries no capability of its own -- it is a plain data assertion.
        binding_field_names = {item.name for item in _dc_fields(TrustedPromotedOperationalTerminalBinding)}
        self.assertEqual(binding_field_names, {"spec_id", "expected_terminal_id"})
        with self.assertRaises(PromotedOperationalServiceError):
            TrustedPromotedOperationalTerminalBinding(
                spec_id="not-hex", expected_terminal_id="0" * 64
            )
        with self.assertRaises(PromotedOperationalServiceError):
            TrustedPromotedOperationalTerminalBinding(
                spec_id="0" * 64, expected_terminal_id="not-hex"
            )


if __name__ == "__main__":
    unittest.main()
