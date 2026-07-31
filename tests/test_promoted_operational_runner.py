from __future__ import annotations

import decimal
import inspect
import unittest
from dataclasses import fields
from datetime import timedelta, timezone
from decimal import Decimal

from india_swing.market_data.models import FullQuoteBatch
from india_swing.promoted_operational_allocation import PromotedOperationalPortfolioContext
from india_swing.promoted_operational_decision import PromotedOperationalDecisionAction
from india_swing.promoted_operational_runner import (
    PromotedOperationalRunFailureCode,
    PromotedOperationalRunResult,
    PromotedOperationalRunSpec,
    PromotedOperationalRunStatus,
    PromotedOperationalRunnerError,
    execute_promoted_operational_run,
)
from india_swing.risk.swing_portfolio import SwingPortfolioSnapshot

from tests import test_promoted_operational_allocation as _allocation_tests
from tests import test_promoted_operational_quote_gate as _quote_gate_tests


_QUOTE_SOURCE_ID = "8" * 64
_PORTFOLIO_SOURCE_ID = "7" * 64

# Reuses the quote-gate test module's own fast in-memory preparation/spec
# fixtures via its instance helper methods, and the allocation test
# module's own policy fixture helper -- the same established
# fixture-reuse convention already used one and two layers down. Imported
# under module aliases so unittest discovery of this module does not also
# collect their TestCase classes. Constructing this instance runs no test
# method; it only makes its instance helper methods callable.
_QUOTE_GATE_FIXTURE = _quote_gate_tests.PromotedOperationalQuoteGateTests(
    methodName="test_fresh_in_window_limit_compatible_quotes_pass_with_exact_reference_ask"
)

_STARTED_AT = _quote_gate_tests._DECISION_NOT_BEFORE + timedelta(minutes=1)
_RUN_EVALUATED_AT = _quote_gate_tests._EVALUATED_AT
_COMPLETED_AT = _quote_gate_tests._EVALUATED_AT + timedelta(minutes=1)
_PORTFOLIO_AS_OF = _quote_gate_tests._EVALUATED_AT - timedelta(seconds=2)


def _clock(*values):
    iterator = iter(values)

    def _next():
        return next(iterator)

    return _next


class _FakeQuoteSource:
    def __init__(self, *, responder, source_id: str = _QUOTE_SOURCE_ID) -> None:
        self._source_id = source_id
        self._responder = responder
        self.calls: list[tuple[str, ...]] = []
        self.source_id_reads = 0

    @property
    def source_id(self) -> str:
        self.source_id_reads += 1
        return self._source_id

    def fetch_full_quotes(self, listing_keys: tuple[str, ...]) -> FullQuoteBatch:
        self.calls.append(listing_keys)
        return self._responder(listing_keys)


class _FakePortfolioSource:
    def __init__(self, *, responder, source_id: str = _PORTFOLIO_SOURCE_ID) -> None:
        self._source_id = source_id
        self._responder = responder
        self.calls = 0
        self.source_id_reads = 0

    @property
    def source_id(self) -> str:
        self.source_id_reads += 1
        return self._source_id

    def read_portfolio_context(self) -> PromotedOperationalPortfolioContext:
        self.calls += 1
        return self._responder()


def _quote_by_key(preparation):
    return {
        quote.listing_key: quote
        for quote in _QUOTE_GATE_FIXTURE._happy_quotes(preparation)
    }


def _quote_batch_for(preparation, listing_keys, *, provider_version="kiteconnect/5.2.0"):
    by_key = _quote_by_key(preparation)
    return FullQuoteBatch(
        requested_keys=listing_keys,
        requested_at=_RUN_EVALUATED_AT - timedelta(seconds=3),
        observed_at=_RUN_EVALUATED_AT - timedelta(seconds=1),
        provider_version=provider_version,
        quotes=tuple(by_key[key] for key in listing_keys),
    )


def _sorted_quote_responder(preparation):
    def _respond(listing_keys):
        return _quote_batch_for(preparation, listing_keys)

    return _respond


def _portfolio_context(*, as_of, open_listing_keys=(), **overrides):
    values = dict(
        capital=Decimal("100000"),
        cash_available=Decimal("100000"),
        gross_exposure=Decimal("0"),
        open_risk=Decimal("0"),
        open_positions=len(open_listing_keys),
        daily_realized_pnl=Decimal("0"),
        pilot_realized_pnl=Decimal("0"),
        as_of=as_of,
    )
    values.update(overrides)
    portfolio = SwingPortfolioSnapshot(**values)
    return PromotedOperationalPortfolioContext(
        portfolio=portfolio,
        source_portfolio_artifact_id=_PORTFOLIO_SOURCE_ID,
        open_listing_keys=tuple(open_listing_keys),
    )


def _happy_portfolio_source() -> _FakePortfolioSource:
    return _FakePortfolioSource(responder=lambda: _portfolio_context(as_of=_PORTFOLIO_AS_OF))


def _run_spec(*, preparation=None, chunk_size=500, allocation_policy=None, quote_gate_policy=None):
    preparation = preparation if preparation is not None else _QUOTE_GATE_FIXTURE._preparation()
    quote_gate_spec = _QUOTE_GATE_FIXTURE._spec(preparation, policy=quote_gate_policy)
    policy = (
        allocation_policy
        if allocation_policy is not None
        else _allocation_tests._allocation_policy()
    )
    spec = PromotedOperationalRunSpec(
        quote_gate_spec=quote_gate_spec,
        allocation_policy=policy,
        expected_quote_source_id=_QUOTE_SOURCE_ID,
        expected_portfolio_source_id=_PORTFOLIO_SOURCE_ID,
        maximum_quote_chunk_size=chunk_size,
    )
    return preparation, spec


class PromotedOperationalRunnerTests(unittest.TestCase):
    def test_happy_path_chunks_sorted_keys_and_builds_exact_complete_paper_decision_chain(
        self,
    ) -> None:
        preparation, spec = _run_spec(chunk_size=1)
        quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
        portfolio_source = _happy_portfolio_source()
        clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)

        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )

        self.assertEqual(result.status, PromotedOperationalRunStatus.COMPLETE)
        self.assertEqual(result.failure_codes, ())
        self.assertEqual(quote_source.calls, [("NSE:RELIANCE",), ("NSE:TCS",)])
        self.assertEqual(portfolio_source.calls, 1)
        self.assertIsNotNone(result.decision_package)
        self.assertEqual(result.quote_batch.requested_keys, ("NSE:RELIANCE", "NSE:TCS"))
        self.assertEqual(result.evaluated_at, _RUN_EVALUATED_AT.astimezone(timezone.utc))
        self.assertEqual(result.quote_source_id, _QUOTE_SOURCE_ID)
        self.assertEqual(result.portfolio_source_id, _PORTFOLIO_SOURCE_ID)
        self.assertTrue(result.paper_only)
        self.assertFalse(result.notification_eligible)
        self.assertFalse(result.execution_eligible)
        result.verify_content_identity()

    def test_zero_candidate_run_skips_quote_source_and_returns_complete_no_trade(self) -> None:
        preparation = _QUOTE_GATE_FIXTURE._empty_preparation()
        _, spec = _run_spec(preparation=preparation)

        def _explode(_keys):
            raise AssertionError("quote source must not be called for a zero-candidate run")

        quote_source = _FakeQuoteSource(responder=_explode)
        portfolio_source = _happy_portfolio_source()
        clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)

        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )

        self.assertEqual(result.status, PromotedOperationalRunStatus.COMPLETE)
        self.assertEqual(quote_source.calls, [])
        self.assertIsNone(result.quote_batch)
        self.assertIsNotNone(result.decision_package)
        self.assertEqual(
            result.decision_package.decision.action, PromotedOperationalDecisionAction.NO_TRADE
        )
        result.verify_content_identity()

    def test_window_and_source_identity_failures_make_no_acquisition_calls(self) -> None:
        _, spec = _run_spec(chunk_size=500)

        def _explode_quotes(_keys):
            raise AssertionError("no acquisition call is expected")

        def _explode_portfolio():
            raise AssertionError("no acquisition call is expected")

        # Out-of-window starts must reject before either source_id property
        # is ever read -- source properties are themselves untrusted
        # capabilities, not just the acquisition methods.
        quote_source = _FakeQuoteSource(responder=_explode_quotes)
        portfolio_source = _FakePortfolioSource(responder=_explode_portfolio)
        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=_clock(_quote_gate_tests._DECISION_NOT_BEFORE - timedelta(minutes=1)),
        )
        self.assertIn(
            PromotedOperationalRunFailureCode.START_BEFORE_WINDOW.value, result.failure_codes
        )
        self.assertEqual(quote_source.source_id_reads, 0)
        self.assertEqual(portfolio_source.source_id_reads, 0)
        self.assertEqual(quote_source.calls, [])
        self.assertEqual(portfolio_source.calls, 0)
        self.assertIsNone(result.quote_source_id)
        self.assertIsNone(result.portfolio_source_id)
        result.verify_content_identity()

        quote_source = _FakeQuoteSource(responder=_explode_quotes)
        portfolio_source = _FakePortfolioSource(responder=_explode_portfolio)
        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=_clock(_quote_gate_tests._DECISION_DEADLINE + timedelta(minutes=1)),
        )
        self.assertIn(
            PromotedOperationalRunFailureCode.START_AFTER_DEADLINE.value, result.failure_codes
        )
        self.assertEqual(quote_source.source_id_reads, 0)
        self.assertEqual(portfolio_source.source_id_reads, 0)
        self.assertEqual(quote_source.calls, [])
        self.assertEqual(portfolio_source.calls, 0)
        self.assertIsNone(result.quote_source_id)
        self.assertIsNone(result.portfolio_source_id)
        result.verify_content_identity()

        # An in-window start with a mismatched quote-source identity reads
        # both source_id properties (validated before either acquisition
        # method) but calls neither acquisition method.
        quote_source = _FakeQuoteSource(source_id="1" * 64, responder=_explode_quotes)
        portfolio_source = _FakePortfolioSource(responder=_explode_portfolio)
        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=_clock(_STARTED_AT),
        )
        self.assertEqual(result.status, PromotedOperationalRunStatus.FAILED)
        self.assertIn(
            PromotedOperationalRunFailureCode.SOURCE_IDENTITY_INVALID.value, result.failure_codes
        )
        self.assertGreaterEqual(quote_source.source_id_reads, 1)
        self.assertGreaterEqual(portfolio_source.source_id_reads, 1)
        self.assertEqual(quote_source.calls, [])
        self.assertEqual(portfolio_source.calls, 0)
        self.assertEqual(result.quote_source_id, "1" * 64)
        self.assertEqual(result.portfolio_source_id, _PORTFOLIO_SOURCE_ID)
        result.verify_content_identity()

    def test_out_of_window_initial_time_reads_neither_source_property_nor_acquisition_method(
        self,
    ) -> None:
        _, spec = _run_spec(chunk_size=500)

        def _explode_quotes(_keys):
            raise AssertionError("acquisition must not be called out of window")

        def _explode_portfolio():
            raise AssertionError("acquisition must not be called out of window")

        for clock_value, expected_code in (
            (
                _quote_gate_tests._DECISION_NOT_BEFORE - timedelta(minutes=1),
                PromotedOperationalRunFailureCode.START_BEFORE_WINDOW,
            ),
            (
                _quote_gate_tests._DECISION_DEADLINE + timedelta(minutes=1),
                PromotedOperationalRunFailureCode.START_AFTER_DEADLINE,
            ),
        ):
            with self.subTest(clock_value=clock_value):
                quote_source = _FakeQuoteSource(responder=_explode_quotes)
                portfolio_source = _FakePortfolioSource(responder=_explode_portfolio)
                result = execute_promoted_operational_run(
                    spec=spec,
                    quote_source=quote_source,
                    portfolio_source=portfolio_source,
                    clock=_clock(clock_value),
                )
                self.assertEqual(quote_source.source_id_reads, 0)
                self.assertEqual(portfolio_source.source_id_reads, 0)
                self.assertEqual(quote_source.calls, [])
                self.assertEqual(portfolio_source.calls, 0)
                self.assertIn(expected_code.value, result.failure_codes)
                self.assertIsNone(result.quote_source_id)
                self.assertIsNone(result.portfolio_source_id)
                self.assertIsNone(result.evaluated_at)
                result.verify_content_identity()

    def test_quote_source_exception_malformed_chunk_wrong_coverage_and_inconsistent_provider_fail_sanitized(
        self,
    ) -> None:
        preparation, spec = _run_spec(chunk_size=500)

        def _run(responder):
            quote_source = _FakeQuoteSource(responder=responder)
            portfolio_source = _happy_portfolio_source()
            clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)
            return execute_promoted_operational_run(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
            )

        with self.subTest(case="source_exception"):
            def _boom(_keys):
                raise RuntimeError("network exploded with secret token abc123")

            result = _run(_boom)
            self.assertIn(
                PromotedOperationalRunFailureCode.QUOTE_ACQUISITION_FAILED.value,
                result.failure_codes,
            )
            self.assertIsNone(result.quote_batch)
            for code in result.failure_codes:
                self.assertNotIn("abc123", code)
            result.verify_content_identity()

        with self.subTest(case="wrong_type"):
            result = _run(lambda _keys: "not a batch")
            self.assertIn(
                PromotedOperationalRunFailureCode.QUOTE_COVERAGE_INVALID.value,
                result.failure_codes,
            )
            result.verify_content_identity()

        with self.subTest(case="wrong_coverage"):
            def _short_coverage(keys):
                return _quote_batch_for(preparation, keys[:1])

            result = _run(_short_coverage)
            self.assertIn(
                PromotedOperationalRunFailureCode.QUOTE_COVERAGE_INVALID.value,
                result.failure_codes,
            )

        with self.subTest(case="inconsistent_provider"):
            _, chunked_spec = _run_spec(chunk_size=1)
            calls = {"n": 0}

            def _inconsistent(keys):
                calls["n"] += 1
                version = "kiteconnect/5.2.0" if calls["n"] == 1 else "kiteconnect/9.9.9"
                return _quote_batch_for(preparation, keys, provider_version=version)

            quote_source = _FakeQuoteSource(responder=_inconsistent)
            portfolio_source = _happy_portfolio_source()
            clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)
            result = execute_promoted_operational_run(
                spec=chunked_spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
            )
            self.assertIn(
                PromotedOperationalRunFailureCode.QUOTE_COVERAGE_INVALID.value,
                result.failure_codes,
            )

    def test_portfolio_exception_wrong_type_source_artifact_mismatch_future_and_stale_context_fail_closed(
        self,
    ) -> None:
        preparation, spec = _run_spec(chunk_size=500)

        def _run(responder):
            quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
            portfolio_source = _FakePortfolioSource(responder=responder)
            clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)
            return execute_promoted_operational_run(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
            )

        with self.subTest(case="source_exception"):
            def _boom():
                raise RuntimeError("broker down, token xyz")

            result = _run(_boom)
            self.assertIn(
                PromotedOperationalRunFailureCode.PORTFOLIO_ACQUISITION_FAILED.value,
                result.failure_codes,
            )
            result.verify_content_identity()

        with self.subTest(case="wrong_type"):
            result = _run(lambda: object())
            self.assertIn(
                PromotedOperationalRunFailureCode.PORTFOLIO_ACQUISITION_FAILED.value,
                result.failure_codes,
            )

        with self.subTest(case="artifact_source_mismatch"):
            def _mismatched():
                context = _portfolio_context(as_of=_PORTFOLIO_AS_OF)
                object.__setattr__(context, "source_portfolio_artifact_id", "1" * 64)
                object.__setattr__(context, "context_id", context._calculated_id())
                return context

            result = _run(_mismatched)
            self.assertIn(
                PromotedOperationalRunFailureCode.PORTFOLIO_ACQUISITION_FAILED.value,
                result.failure_codes,
            )

        with self.subTest(case="future_portfolio_caught_by_runner_monotonic_check"):
            # A portfolio artifact claiming a timestamp after evaluated_at is a
            # lookahead risk the runner itself gates on, independent of the
            # allocation module's own separate maximum-age policy check below.
            def _future():
                return _portfolio_context(as_of=_RUN_EVALUATED_AT + timedelta(minutes=5))

            result = _run(_future)
            self.assertIn(
                PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value, result.failure_codes
            )

        with self.subTest(case="stale_portfolio_caught_by_allocation_policy_age_check"):
            # Older than the allocation policy's own maximum_portfolio_age_seconds
            # (300s default) but still chronologically before evaluated_at, so the
            # runner's own monotonic check does not fire; allocation's own
            # staleness check does.
            def _stale():
                return _portfolio_context(as_of=_RUN_EVALUATED_AT - timedelta(seconds=301))

            result = _run(_stale)
            self.assertIn(
                PromotedOperationalRunFailureCode.ALLOCATION_FAILED.value, result.failure_codes
            )

    def test_non_monotonic_invalid_later_clock_and_deadline_crossing_fail_closed(self) -> None:
        preparation, spec = _run_spec(chunk_size=500)

        def _run(*clock_values):
            quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
            portfolio_source = _happy_portfolio_source()
            clock = _clock(*clock_values)
            return execute_promoted_operational_run(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
            )

        with self.subTest(case="evaluated_at_clock_raises"):
            quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
            portfolio_source = _happy_portfolio_source()
            remaining = iter([_STARTED_AT])

            def _flaky_clock():
                try:
                    return next(remaining)
                except StopIteration:
                    raise RuntimeError("clock broke") from None

            result = execute_promoted_operational_run(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=_flaky_clock,
            )
            self.assertIn(
                PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value, result.failure_codes
            )
            result.verify_content_identity()

        with self.subTest(case="evaluated_at_before_started_at"):
            result = _run(_STARTED_AT, _STARTED_AT - timedelta(minutes=5), _COMPLETED_AT)
            self.assertIn(
                PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value, result.failure_codes
            )

        with self.subTest(case="evaluation_after_deadline"):
            result = _run(
                _STARTED_AT,
                _quote_gate_tests._DECISION_DEADLINE + timedelta(seconds=1),
                _COMPLETED_AT,
            )
            self.assertIn(
                PromotedOperationalRunFailureCode.EVALUATION_AFTER_DEADLINE.value,
                result.failure_codes,
            )

        with self.subTest(case="completed_at_before_evaluated_at"):
            result = _run(
                _STARTED_AT, _RUN_EVALUATED_AT, _RUN_EVALUATED_AT - timedelta(minutes=2)
            )
            self.assertIn(
                PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value, result.failure_codes
            )
            self.assertIsNone(result.decision_package)
            self.assertIsNotNone(result.allocation_batch)

        with self.subTest(case="completion_after_deadline"):
            result = _run(
                _STARTED_AT,
                _RUN_EVALUATED_AT,
                _quote_gate_tests._DECISION_DEADLINE + timedelta(seconds=1),
            )
            self.assertIn(
                PromotedOperationalRunFailureCode.COMPLETION_AFTER_DEADLINE.value,
                result.failure_codes,
            )
            self.assertIsNone(result.decision_package)
            self.assertIsNotNone(result.allocation_batch)

    def test_multiple_allocations_become_sanitized_decision_assembly_failure_without_trade_package(
        self,
    ) -> None:
        preparation, spec = _run_spec(
            chunk_size=500,
            allocation_policy=_allocation_tests._allocation_policy(
                policy=_allocation_tests.SwingPortfolioSizingPolicy(
                    maximum_new_positions_per_run=2
                )
            ),
        )
        quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
        portfolio_source = _happy_portfolio_source()
        clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)

        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )

        self.assertEqual(result.status, PromotedOperationalRunStatus.FAILED)
        self.assertIn(
            PromotedOperationalRunFailureCode.DECISION_ASSEMBLY_FAILED.value,
            result.failure_codes,
        )
        self.assertIsNone(result.decision_package)
        self.assertIsNotNone(result.allocation_batch)
        self.assertEqual(result.allocation_batch.allocated_count, 2)
        result.verify_content_identity()

    def test_failed_results_retain_only_exact_verified_prefix_and_never_exception_text(
        self,
    ) -> None:
        _, spec = _run_spec(chunk_size=500)

        def _boom(_keys):
            raise RuntimeError("leaked-secret-token-zzz")

        quote_source = _FakeQuoteSource(responder=_boom)
        portfolio_source = _happy_portfolio_source()
        clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)

        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )

        self.assertEqual(result.status, PromotedOperationalRunStatus.FAILED)
        self.assertIsNone(result.quote_batch)
        self.assertIsNone(result.portfolio_context)
        self.assertIsNone(result.quote_gate_batch)
        self.assertIsNone(result.allocation_batch)
        self.assertIsNone(result.decision_package)
        joined = " ".join(result.failure_codes)
        self.assertNotIn("leaked-secret-token-zzz", joined)
        for code in result.failure_codes:
            self.assertIn(code, {value.value for value in PromotedOperationalRunFailureCode})
        result.verify_content_identity()

    def test_direct_construction_nested_mutation_missing_middle_layer_authority_schema_status_and_self_consistent_id_forgery_fail_closed(
        self,
    ) -> None:
        preparation, spec = _run_spec(chunk_size=500)
        quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
        portfolio_source = _happy_portfolio_source()
        clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)
        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )
        self.assertEqual(result.status, PromotedOperationalRunStatus.COMPLETE)

        base_kwargs = dict(
            spec=result.spec,
            quote_source_id=result.quote_source_id,
            portfolio_source_id=result.portfolio_source_id,
            started_at=result.started_at,
            evaluated_at=result.evaluated_at,
            completed_at=result.completed_at,
            quote_batch=result.quote_batch,
            portfolio_context=result.portfolio_context,
            quote_gate_batch=result.quote_gate_batch,
            allocation_batch=result.allocation_batch,
            decision_package=result.decision_package,
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )

        # Missing middle layer: decision_package present but allocation_batch missing.
        with self.assertRaises(PromotedOperationalRunnerError):
            PromotedOperationalRunResult(
                **{
                    **base_kwargs,
                    "status": PromotedOperationalRunStatus.COMPLETE,
                    "failure_codes": (),
                    "allocation_batch": None,
                }
            )

        # Changed status: COMPLETE claimed alongside a nonempty failure code.
        with self.assertRaises(PromotedOperationalRunnerError):
            PromotedOperationalRunResult(
                **{
                    **base_kwargs,
                    "status": PromotedOperationalRunStatus.COMPLETE,
                    "failure_codes": ("QUOTE_GATE_FAILED",),
                }
            )

        # Changed authority flag.
        with self.assertRaises(PromotedOperationalRunnerError):
            PromotedOperationalRunResult(
                **{
                    **base_kwargs,
                    "status": PromotedOperationalRunStatus.COMPLETE,
                    "failure_codes": (),
                    "notification_eligible": True,
                }
            )

        # Self-consistent nested mutation: mutate deep inside the retained
        # allocation batch's final_state without touching the outer result's
        # own stored ID -- the outer ID stays stale, but verification fails.
        original_result_id = result.result_id
        object.__setattr__(result.allocation_batch.final_state, "cash_available", Decimal("-1"))
        self.assertEqual(result.result_id, original_result_id)
        with self.assertRaises(Exception):
            result.verify_content_identity()

        # Reordered/duplicated failure codes on a fresh FAILED result.
        _, failed_spec = _run_spec(chunk_size=500)

        def _boom(_keys):
            raise RuntimeError("boom")

        failed_result = execute_promoted_operational_run(
            spec=failed_spec,
            quote_source=_FakeQuoteSource(responder=_boom),
            portfolio_source=_happy_portfolio_source(),
            clock=_clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT),
        )
        self.assertEqual(failed_result.status, PromotedOperationalRunStatus.FAILED)
        with self.assertRaises(PromotedOperationalRunnerError):
            PromotedOperationalRunResult(
                spec=failed_result.spec,
                quote_source_id=failed_result.quote_source_id,
                portfolio_source_id=failed_result.portfolio_source_id,
                started_at=failed_result.started_at,
                evaluated_at=failed_result.evaluated_at,
                completed_at=failed_result.completed_at,
                status=PromotedOperationalRunStatus.FAILED,
                failure_codes=failed_result.failure_codes + failed_result.failure_codes,
                quote_batch=failed_result.quote_batch,
                portfolio_context=failed_result.portfolio_context,
                quote_gate_batch=failed_result.quote_gate_batch,
                allocation_batch=failed_result.allocation_batch,
                decision_package=None,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        # Self-consistent RunSpec forgery: tamper the chunk ceiling out of
        # range and recompute a self-consistent spec_id -- still fails,
        # since verify_content_identity reconstructs a fresh spec from the
        # (now tampered) retained fields and that reconstruction itself
        # re-runs the [1, 500] bound.
        tampered_spec = PromotedOperationalRunSpec(
            quote_gate_spec=spec.quote_gate_spec,
            allocation_policy=spec.allocation_policy,
            expected_quote_source_id=spec.expected_quote_source_id,
            expected_portfolio_source_id=spec.expected_portfolio_source_id,
            maximum_quote_chunk_size=spec.maximum_quote_chunk_size,
        )
        object.__setattr__(tampered_spec, "maximum_quote_chunk_size", 999)
        object.__setattr__(tampered_spec, "spec_id", tampered_spec._calculated_id())
        with self.assertRaises(PromotedOperationalRunnerError):
            tampered_spec.verify_content_identity()

    def test_deterministic_result_ids_and_ambient_decimal_context_independence(self) -> None:
        preparation, spec = _run_spec(chunk_size=500)

        def _execute():
            quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
            portfolio_source = _happy_portfolio_source()
            clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)
            return execute_promoted_operational_run(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
            )

        result_a = _execute()
        result_b = _execute()
        self.assertEqual(result_a.result_id, result_b.result_id)
        self.assertEqual(
            result_a.decision_package.package_id, result_b.decision_package.package_id
        )

        original_precision = decimal.getcontext().prec
        decimal.getcontext().prec = 1
        try:
            result_a.verify_content_identity()
        finally:
            decimal.getcontext().prec = original_precision
        self.assertEqual(decimal.getcontext().prec, original_precision)
        result_a.verify_content_identity()

    def test_result_schema_self_consistent_forgery_fails_replay(self) -> None:
        preparation, spec = _run_spec(chunk_size=500)
        quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
        portfolio_source = _happy_portfolio_source()
        clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)
        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )
        self.assertEqual(result.status, PromotedOperationalRunStatus.COMPLETE)

        object.__setattr__(result, "schema_version", "forged/v999")
        object.__setattr__(result, "result_id", result._calculated_id())
        with self.assertRaises(PromotedOperationalRunnerError):
            result.verify_content_identity()

    def test_complete_result_requires_start_inside_exact_decision_window(self) -> None:
        preparation, spec = _run_spec(chunk_size=500)
        quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
        portfolio_source = _happy_portfolio_source()
        clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)
        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )
        self.assertEqual(result.status, PromotedOperationalRunStatus.COMPLETE)

        for tampered_started_at in (
            _quote_gate_tests._DECISION_NOT_BEFORE - timedelta(seconds=1),
            _quote_gate_tests._DECISION_DEADLINE + timedelta(seconds=1),
        ):
            with self.subTest(tampered_started_at=tampered_started_at):
                tampered = PromotedOperationalRunResult(
                    spec=result.spec,
                    quote_source_id=result.quote_source_id,
                    portfolio_source_id=result.portfolio_source_id,
                    started_at=result.started_at,
                    evaluated_at=result.evaluated_at,
                    completed_at=result.completed_at,
                    status=PromotedOperationalRunStatus.COMPLETE,
                    failure_codes=(),
                    quote_batch=result.quote_batch,
                    portfolio_context=result.portfolio_context,
                    quote_gate_batch=result.quote_gate_batch,
                    allocation_batch=result.allocation_batch,
                    decision_package=result.decision_package,
                    paper_only=True,
                    notification_eligible=False,
                    execution_eligible=False,
                )
                object.__setattr__(
                    tampered, "started_at", tampered_started_at.astimezone(timezone.utc)
                )
                object.__setattr__(tampered, "result_id", tampered._calculated_id())
                with self.assertRaises(PromotedOperationalRunnerError):
                    tampered.verify_content_identity()

    def test_failure_code_truth_and_exact_stage_prefix_reject_impossible_self_consistent_results(
        self,
    ) -> None:
        preparation, spec = _run_spec(chunk_size=500)
        quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
        portfolio_source = _happy_portfolio_source()
        clock = _clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT)
        complete = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )
        self.assertEqual(complete.status, PromotedOperationalRunStatus.COMPLETE)

        base = dict(
            spec=complete.spec,
            quote_source_id=complete.quote_source_id,
            portfolio_source_id=complete.portfolio_source_id,
            started_at=complete.started_at,
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )

        with self.subTest(case="quote_acquisition_failed_retaining_allocation"):
            with self.assertRaises(PromotedOperationalRunnerError):
                PromotedOperationalRunResult(
                    **base,
                    evaluated_at=None,
                    completed_at=complete.started_at,
                    status=PromotedOperationalRunStatus.FAILED,
                    failure_codes=(
                        PromotedOperationalRunFailureCode.QUOTE_ACQUISITION_FAILED.value,
                    ),
                    quote_batch=None,
                    portfolio_context=None,
                    quote_gate_batch=None,
                    allocation_batch=complete.allocation_batch,
                    decision_package=None,
                )

        with self.subTest(case="portfolio_acquisition_failed_retaining_portfolio_context"):
            with self.assertRaises(PromotedOperationalRunnerError):
                PromotedOperationalRunResult(
                    **base,
                    evaluated_at=None,
                    completed_at=complete.started_at,
                    status=PromotedOperationalRunStatus.FAILED,
                    failure_codes=(
                        PromotedOperationalRunFailureCode.PORTFOLIO_ACQUISITION_FAILED.value,
                    ),
                    quote_batch=complete.quote_batch,
                    portfolio_context=complete.portfolio_context,
                    quote_gate_batch=None,
                    allocation_batch=None,
                    decision_package=None,
                )

        with self.subTest(case="quote_gate_failed_retaining_quote_gate_batch"):
            with self.assertRaises(PromotedOperationalRunnerError):
                PromotedOperationalRunResult(
                    **base,
                    evaluated_at=complete.evaluated_at,
                    completed_at=complete.completed_at,
                    status=PromotedOperationalRunStatus.FAILED,
                    failure_codes=(PromotedOperationalRunFailureCode.QUOTE_GATE_FAILED.value,),
                    quote_batch=complete.quote_batch,
                    portfolio_context=complete.portfolio_context,
                    quote_gate_batch=complete.quote_gate_batch,
                    allocation_batch=None,
                    decision_package=None,
                )

        with self.subTest(case="allocation_failed_retaining_allocation"):
            with self.assertRaises(PromotedOperationalRunnerError):
                PromotedOperationalRunResult(
                    **base,
                    evaluated_at=complete.evaluated_at,
                    completed_at=complete.completed_at,
                    status=PromotedOperationalRunStatus.FAILED,
                    failure_codes=(PromotedOperationalRunFailureCode.ALLOCATION_FAILED.value,),
                    quote_batch=complete.quote_batch,
                    portfolio_context=complete.portfolio_context,
                    quote_gate_batch=complete.quote_gate_batch,
                    allocation_batch=complete.allocation_batch,
                    decision_package=None,
                )

        with self.subTest(case="evaluation_after_deadline_with_in_deadline_evaluated_at"):
            with self.assertRaises(PromotedOperationalRunnerError):
                PromotedOperationalRunResult(
                    **base,
                    evaluated_at=complete.evaluated_at,
                    completed_at=complete.completed_at,
                    status=PromotedOperationalRunStatus.FAILED,
                    failure_codes=(
                        PromotedOperationalRunFailureCode.EVALUATION_AFTER_DEADLINE.value,
                    ),
                    quote_batch=complete.quote_batch,
                    portfolio_context=complete.portfolio_context,
                    quote_gate_batch=None,
                    allocation_batch=None,
                    decision_package=None,
                )

        with self.subTest(case="completion_after_deadline_with_in_deadline_completed_at"):
            with self.assertRaises(PromotedOperationalRunnerError):
                PromotedOperationalRunResult(
                    **base,
                    evaluated_at=complete.evaluated_at,
                    completed_at=complete.completed_at,
                    status=PromotedOperationalRunStatus.FAILED,
                    failure_codes=(
                        PromotedOperationalRunFailureCode.COMPLETION_AFTER_DEADLINE.value,
                    ),
                    quote_batch=complete.quote_batch,
                    portfolio_context=complete.portfolio_context,
                    quote_gate_batch=complete.quote_gate_batch,
                    allocation_batch=complete.allocation_batch,
                    decision_package=None,
                )

        with self.subTest(case="source_identity_invalid_with_both_ids_matching"):
            with self.assertRaises(PromotedOperationalRunnerError):
                PromotedOperationalRunResult(
                    **base,
                    evaluated_at=None,
                    completed_at=complete.started_at,
                    status=PromotedOperationalRunStatus.FAILED,
                    failure_codes=(
                        PromotedOperationalRunFailureCode.SOURCE_IDENTITY_INVALID.value,
                    ),
                    quote_batch=None,
                    portfolio_context=None,
                    quote_gate_batch=None,
                    allocation_batch=None,
                    decision_package=None,
                )

        with self.subTest(case="clock_only_cannot_skip_quote_before_portfolio"):
            with self.assertRaises(PromotedOperationalRunnerError):
                PromotedOperationalRunResult(
                    **base,
                    evaluated_at=complete.evaluated_at,
                    completed_at=complete.completed_at,
                    status=PromotedOperationalRunStatus.FAILED,
                    failure_codes=(
                        PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value,
                    ),
                    quote_batch=None,
                    portfolio_context=complete.portfolio_context,
                    quote_gate_batch=None,
                    allocation_batch=None,
                    decision_package=None,
                )

        for case, primary_code, evaluated_at, completed_at, quote_gate_batch, allocation_batch in (
            (
                "evaluation_deadline_cannot_combine_with_clock",
                PromotedOperationalRunFailureCode.EVALUATION_AFTER_DEADLINE.value,
                spec.quote_gate_spec.decision_deadline + timedelta(seconds=1),
                spec.quote_gate_spec.decision_deadline + timedelta(seconds=1),
                None,
                None,
            ),
            (
                "completion_deadline_cannot_combine_with_clock",
                PromotedOperationalRunFailureCode.COMPLETION_AFTER_DEADLINE.value,
                complete.evaluated_at,
                spec.quote_gate_spec.decision_deadline + timedelta(seconds=1),
                complete.quote_gate_batch,
                complete.allocation_batch,
            ),
        ):
            with self.subTest(case=case):
                with self.assertRaises(PromotedOperationalRunnerError):
                    PromotedOperationalRunResult(
                        **base,
                        evaluated_at=evaluated_at,
                        completed_at=completed_at,
                        status=PromotedOperationalRunStatus.FAILED,
                        failure_codes=tuple(
                            sorted(
                                (
                                    PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value,
                                    primary_code,
                                )
                            )
                        ),
                        quote_batch=complete.quote_batch,
                        portfolio_context=complete.portfolio_context,
                        quote_gate_batch=quote_gate_batch,
                        allocation_batch=allocation_batch,
                        decision_package=None,
                    )

        with self.subTest(case="self_consistent_recompute_forgery"):
            def _boom(_keys):
                raise RuntimeError("boom")

            real_failed = execute_promoted_operational_run(
                spec=spec,
                quote_source=_FakeQuoteSource(responder=_boom),
                portfolio_source=_happy_portfolio_source(),
                clock=_clock(_STARTED_AT, _RUN_EVALUATED_AT, _COMPLETED_AT),
            )
            self.assertIn(
                PromotedOperationalRunFailureCode.QUOTE_ACQUISITION_FAILED.value,
                real_failed.failure_codes,
            )
            object.__setattr__(real_failed, "allocation_batch", complete.allocation_batch)
            object.__setattr__(real_failed, "result_id", real_failed._calculated_id())
            with self.assertRaises(PromotedOperationalRunnerError):
                real_failed.verify_content_identity()

    def test_valid_failure_stage_prefixes_and_clock_combinations_still_replay(self) -> None:
        preparation, spec = _run_spec(chunk_size=500)

        with self.subTest(case="root_failure_plus_clock_non_monotonic"):
            def _boom(_keys):
                raise RuntimeError("boom")

            remaining = iter([_STARTED_AT])

            def _flaky_clock():
                try:
                    return next(remaining)
                except StopIteration:
                    raise RuntimeError("clock broke") from None

            result = execute_promoted_operational_run(
                spec=spec,
                quote_source=_FakeQuoteSource(responder=_boom),
                portfolio_source=_happy_portfolio_source(),
                clock=_flaky_clock,
            )
            self.assertEqual(
                set(result.failure_codes),
                {
                    PromotedOperationalRunFailureCode.QUOTE_ACQUISITION_FAILED.value,
                    PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value,
                },
            )
            result.verify_content_identity()
            for code in result.failure_codes:
                self.assertNotIn("boom", code)
                self.assertNotIn("clock broke", code)

        with self.subTest(case="clock_non_monotonic_alone_at_evaluation"):
            quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
            portfolio_source = _happy_portfolio_source()
            result = execute_promoted_operational_run(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=_clock(_STARTED_AT, _STARTED_AT - timedelta(minutes=5), _COMPLETED_AT),
            )
            self.assertEqual(
                result.failure_codes,
                (PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value,),
            )
            self.assertIsNone(result.quote_gate_batch)
            self.assertIsNotNone(result.quote_batch)
            self.assertIsNotNone(result.portfolio_context)
            result.verify_content_identity()

        with self.subTest(case="clock_non_monotonic_alone_after_allocation"):
            quote_source = _FakeQuoteSource(responder=_sorted_quote_responder(preparation))
            portfolio_source = _happy_portfolio_source()
            result = execute_promoted_operational_run(
                spec=spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=_clock(
                    _STARTED_AT, _RUN_EVALUATED_AT, _RUN_EVALUATED_AT - timedelta(minutes=2)
                ),
            )
            self.assertEqual(
                result.failure_codes,
                (PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value,),
            )
            self.assertIsNotNone(result.allocation_batch)
            self.assertIsNone(result.decision_package)
            result.verify_content_identity()

    def test_runner_public_contract_has_no_persistence_notification_broker_environment_filesystem_or_order_capability(
        self,
    ) -> None:
        import india_swing.promoted_operational_runner as runner_module

        source = inspect.getsource(runner_module)
        for forbidden in (
            "os.environ",
            "getenv",
            "open(",
            "socket",
            "requests",
            "kiteconnect",
            "boto3",
            "google.cloud",
            "telegram",
            "datetime.now",
            "utcnow",
        ):
            self.assertNotIn(forbidden, source)

        signature = inspect.signature(execute_promoted_operational_run)
        self.assertEqual(
            set(signature.parameters), {"spec", "quote_source", "portfolio_source", "clock"}
        )

        result_field_names = {item.name for item in fields(PromotedOperationalRunResult)}
        self.assertFalse(any("probability" in name for name in result_field_names))
        self.assertFalse(any("confidence" in name for name in result_field_names))


if __name__ == "__main__":
    unittest.main()
