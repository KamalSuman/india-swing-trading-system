from __future__ import annotations

import io
import hashlib
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import fields, replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from india_swing.identity import content_id
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.market_data.models import FullQuoteBatch
from india_swing.operations import (
    FixedSwingPortfolioSource,
    LocalSwingOperationalRunStore,
    SwingOperationalError,
    SwingOperationalFailureCode,
    SwingOperationalGCSError,
    SwingOperationalRunResult,
    SwingOperationalStatus,
    SwingOperationalStoreError,
    build_swing_operational_run_spec,
    execute_swing_operational_run,
    operational_record_from_result,
    operational_record_object_name,
    publish_operational_record_to_gcs,
    publish_swing_operational_run,
    run_and_publish_swing_operation,
)
from india_swing.operations.cli import main as operational_cli
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.recommendations import (
    LocalSwingDecisionOutbox,
    SwingDecisionAction,
)
from india_swing.risk.swing_portfolio import (
    SwingPortfolioSizingPolicy,
    SwingPortfolioSnapshot,
)

from tests import test_swing_opportunity_ranking as ranking_fixtures


D = Decimal


class SequenceClock:
    def __init__(self, *values) -> None:
        self.values = iter(values)

    def __call__(self):
        return next(self.values)


class FakeQuoteSource:
    def __init__(self, base: FullQuoteBatch, *, fail: bool = False) -> None:
        self.base = base
        self.fail = fail
        self.calls: list[tuple[str, ...]] = []
        self.source_id = content_id(
            {"kind": "FAKE_QUOTE_SOURCE", "base_batch_id": base.batch_id},
            length=64,
        )
        self._by_key = {value.listing_key: value for value in base.quotes}

    def fetch_full_quotes(self, listing_keys: tuple[str, ...]) -> FullQuoteBatch:
        self.calls.append(listing_keys)
        if self.fail:
            raise RuntimeError("secret upstream detail")
        return FullQuoteBatch(
            requested_keys=listing_keys,
            requested_at=self.base.requested_at,
            observed_at=self.base.observed_at,
            provider_version=self.base.provider_version,
            quotes=tuple(self._by_key[value] for value in listing_keys),
        )


class FakePortfolioSource:
    def __init__(self, portfolio: SwingPortfolioSnapshot, *, fail: bool = False) -> None:
        self.portfolio = portfolio
        self.fail = fail
        self.calls = 0
        self.source_id = content_id(
            {
                "kind": "FAKE_PORTFOLIO_SOURCE",
                "portfolio_snapshot_id": portfolio.portfolio_snapshot_id,
            },
            length=64,
        )

    def read_portfolio(self) -> SwingPortfolioSnapshot:
        self.calls += 1
        if self.fail:
            raise RuntimeError("secret account detail")
        return self.portfolio


class WrongCoverageQuoteSource(FakeQuoteSource):
    def fetch_full_quotes(self, listing_keys: tuple[str, ...]) -> FullQuoteBatch:
        self.calls.append(listing_keys)
        return self.base


class FakeStateObjectWriter:
    def __init__(self, *, wrong_path: bool = False) -> None:
        self.wrong_path = wrong_path
        self.calls: list[dict[str, object]] = []

    def create_or_verify(self, **values) -> PublishedStateObject:
        self.calls.append(values)
        payload = values["content_bytes"]
        return PublishedStateObject(
            object_name=("wrong/path.json" if self.wrong_path else values["object_name"]),
            generation=7,
            byte_count=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )


class SwingOperationalRunTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        cls.ranking_fixture = fixture
        cls.quote_fixture = fixture.fixture
        cls.proposal_batch = cls.quote_fixture.proposal_batch
        cls.base_quotes = fixture.quote_batch
        cls.evaluated_at = cls.quote_fixture.evaluated_at
        cls.portfolio = SwingPortfolioSnapshot(
            capital=D("100000"),
            cash_available=D("100000"),
            gross_exposure=D("0"),
            open_risk=D("0"),
            open_positions=0,
            daily_realized_pnl=D("0"),
            pilot_realized_pnl=D("0"),
            as_of=cls.evaluated_at - timedelta(seconds=1),
        )
        cls.spec = build_swing_operational_run_spec(
            proposal_batch=cls.proposal_batch,
            quote_chunk_size=1,
        )

    def _run(
        self,
        *,
        spec=None,
        quote_source=None,
        portfolio_source=None,
        times=None,
    ) -> SwingOperationalRunResult:
        return execute_swing_operational_run(
            spec=spec or self.spec,
            quote_source=quote_source or FakeQuoteSource(self.base_quotes),
            portfolio_source=portfolio_source or FakePortfolioSource(self.portfolio),
            clock=SequenceClock(
                *(times or (
                    self.evaluated_at - timedelta(milliseconds=500),
                    self.evaluated_at,
                    self.evaluated_at + timedelta(milliseconds=500),
                ))
            ),
        )

    def test_spec_is_deterministic_exact_and_bound_to_entry_window(self) -> None:
        second = build_swing_operational_run_spec(
            proposal_batch=self.proposal_batch,
            quote_chunk_size=1,
        )

        self.assertEqual(self.spec.spec_id, second.spec_id)
        self.assertEqual(self.spec.target_session, self.proposal_batch.proposals[0].entry_window.entry_day)
        self.assertEqual(
            self.spec.decision_not_before,
            self.proposal_batch.proposals[0].entry_window.earliest_entry_at,
        )
        self.assertEqual(
            self.spec.decision_deadline,
            self.proposal_batch.proposals[0].entry_window.entry_expires_at,
        )
        self.spec.verify_content_identity()

    def test_spec_rejects_unsafe_chunk_policy_window_and_mutation(self) -> None:
        for chunk_size in (0, 501, True):
            with self.subTest(chunk_size=chunk_size):
                with self.assertRaises(SwingOperationalError):
                    build_swing_operational_run_spec(
                        proposal_batch=self.proposal_batch,
                        quote_chunk_size=chunk_size,
                    )
        with self.assertRaises(SwingOperationalError):
            build_swing_operational_run_spec(
                proposal_batch=self.proposal_batch,
                sizing_policy=replace(
                    SwingPortfolioSizingPolicy(),
                    maximum_new_positions_per_run=2,
                ),
            )
        mutated = build_swing_operational_run_spec(proposal_batch=self.proposal_batch)
        object.__setattr__(mutated, "decision_deadline", mutated.decision_deadline + timedelta(seconds=1))
        with self.assertRaises(SwingOperationalError):
            mutated.verify_content_identity()

    def test_complete_run_chunks_quotes_builds_buy_and_registers_paper_trade(self) -> None:
        quote_source = FakeQuoteSource(self.base_quotes)
        result = self._run(quote_source=quote_source)

        self.assertEqual(quote_source.calls, [("NSE:STOCKA",), ("NSE:STOCKB",)])
        self.assertEqual(result.status, SwingOperationalStatus.COMPLETE)
        self.assertEqual(result.action, SwingDecisionAction.BUY)
        self.assertEqual(result.quote_batch.requested_keys, ("NSE:STOCKA", "NSE:STOCKB"))
        self.assertIsNotNone(result.decision_package)
        self.assertIsNotNone(result.paper_registration)
        self.assertEqual(
            result.paper_registration.source_run_id,
            result.spec.spec_id,
        )
        self.assertEqual(
            result.paper_registration.source_decision_integrity_hash,
            result.decision_package.decision.decision_id,
        )
        self.assertFalse(result.execution_eligible)
        result.verify_content_identity()

    def test_complete_no_trade_has_no_paper_registration(self) -> None:
        portfolio = replace(self.portfolio, cash_available=D("0"))
        result = self._run(portfolio_source=FakePortfolioSource(portfolio))

        self.assertEqual(result.status, SwingOperationalStatus.COMPLETE)
        self.assertEqual(result.action, SwingDecisionAction.NO_TRADE)
        self.assertIsNone(result.paper_registration)
        self.assertIn("Decision: NO_TRADE", result.notification_message)

    def test_start_outside_window_fails_before_any_acquisition(self) -> None:
        cases = (
            (
                self.spec.decision_not_before - timedelta(microseconds=1),
                SwingOperationalFailureCode.START_BEFORE_WINDOW,
            ),
            (
                self.spec.decision_deadline + timedelta(microseconds=1),
                SwingOperationalFailureCode.START_AFTER_DEADLINE,
            ),
        )
        for started_at, code in cases:
            with self.subTest(code=code):
                quote_source = FakeQuoteSource(self.base_quotes)
                portfolio_source = FakePortfolioSource(self.portfolio)
                result = self._run(
                    quote_source=quote_source,
                    portfolio_source=portfolio_source,
                    times=(started_at,),
                )
                self.assertEqual(result.status, SwingOperationalStatus.FAILED)
                self.assertEqual(result.action, SwingDecisionAction.NO_TRADE)
                self.assertEqual(result.failure_codes, (code,))
                self.assertEqual(quote_source.calls, [])
                self.assertEqual(portfolio_source.calls, 0)

    def test_quote_failure_is_sanitized_and_never_reads_portfolio(self) -> None:
        portfolio_source = FakePortfolioSource(self.portfolio)
        result = self._run(
            quote_source=FakeQuoteSource(self.base_quotes, fail=True),
            portfolio_source=portfolio_source,
            times=(self.evaluated_at, self.evaluated_at + timedelta(seconds=1)),
        )

        self.assertEqual(
            result.failure_codes,
            (SwingOperationalFailureCode.QUOTE_ACQUISITION_FAILED,),
        )
        self.assertEqual(portfolio_source.calls, 0)
        self.assertNotIn("secret", result.notification_message)
        self.assertIsNone(result.decision_package)

    def test_wrong_quote_coverage_is_distinguished_from_upstream_failure(self) -> None:
        result = self._run(
            quote_source=WrongCoverageQuoteSource(self.base_quotes),
            times=(self.evaluated_at, self.evaluated_at + timedelta(seconds=1)),
        )

        self.assertEqual(
            result.failure_codes,
            (SwingOperationalFailureCode.QUOTE_COVERAGE_INVALID,),
        )

    def test_portfolio_failure_preserves_quote_lineage_and_is_sanitized(self) -> None:
        result = self._run(
            portfolio_source=FakePortfolioSource(self.portfolio, fail=True),
            times=(self.evaluated_at, self.evaluated_at + timedelta(seconds=1)),
        )

        self.assertEqual(
            result.failure_codes,
            (SwingOperationalFailureCode.PORTFOLIO_ACQUISITION_FAILED,),
        )
        self.assertIsNotNone(result.quote_batch)
        self.assertIsNone(result.portfolio)
        self.assertNotIn("secret", result.notification_message)

    def test_nonmonotonic_clock_and_late_evaluation_fail_closed(self) -> None:
        nonmonotonic = self._run(
            times=(
                self.evaluated_at,
                self.evaluated_at - timedelta(seconds=1),
            )
        )
        self.assertEqual(
            nonmonotonic.failure_codes,
            (SwingOperationalFailureCode.CLOCK_NON_MONOTONIC,),
        )

        late_start = self.spec.decision_deadline - timedelta(seconds=1)
        late = self._run(
            times=(late_start, self.spec.decision_deadline + timedelta(microseconds=1)),
        )
        self.assertEqual(
            late.failure_codes,
            (SwingOperationalFailureCode.EVALUATION_AFTER_DEADLINE,),
        )

        late_completion = self._run(
            times=(
                self.evaluated_at,
                self.evaluated_at + timedelta(milliseconds=1),
                self.spec.decision_deadline + timedelta(microseconds=1),
            )
        )
        self.assertEqual(late_completion.status, SwingOperationalStatus.FAILED)
        self.assertEqual(
            late_completion.failure_codes,
            (SwingOperationalFailureCode.EVALUATION_AFTER_DEADLINE,),
        )
        self.assertIsNone(late_completion.decision_package)

    def test_fixed_portfolio_source_is_content_bound_and_mutation_detected(self) -> None:
        source = FixedSwingPortfolioSource(
            self.portfolio,
            source_version="manual/v1",
        )
        first_id = source.source_id

        self.assertEqual(source.read_portfolio(), self.portfolio)
        self.assertEqual(first_id, source.source_id)
        object.__setattr__(self.portfolio, "cash_available", D("1"))
        try:
            with self.assertRaises(Exception):
                source.read_portfolio()
        finally:
            object.__setattr__(self.portfolio, "cash_available", D("100000"))

    def test_direct_result_forgery_and_nested_mutation_are_rejected(self) -> None:
        result = self._run()
        with self.assertRaises(SwingOperationalError):
            replace(result, action=SwingDecisionAction.NO_TRADE)
        with self.assertRaises(SwingOperationalError):
            replace(result, failure_codes=(SwingOperationalFailureCode.DECISION_ASSEMBLY_FAILED,))

        fresh = self._run()
        original_id = fresh.run_id
        object.__setattr__(fresh.portfolio, "open_risk", D("99999"))
        try:
            self.assertEqual(fresh.run_id, original_id)
            with self.assertRaises(Exception):
                fresh.verify_content_identity()
        finally:
            object.__setattr__(fresh.portfolio, "open_risk", D("0"))


class SwingOperationalPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        quote_fixture = fixture.fixture
        cls.evaluated_at = quote_fixture.evaluated_at
        cls.portfolio = SwingPortfolioSnapshot(
            capital=D("100000"),
            cash_available=D("100000"),
            gross_exposure=D("0"),
            open_risk=D("0"),
            open_positions=0,
            daily_realized_pnl=D("0"),
            pilot_realized_pnl=D("0"),
            as_of=cls.evaluated_at - timedelta(seconds=1),
        )
        cls.spec = build_swing_operational_run_spec(
            proposal_batch=quote_fixture.proposal_batch,
        )
        cls.result = execute_swing_operational_run(
            spec=cls.spec,
            quote_source=FakeQuoteSource(fixture.quote_batch),
            portfolio_source=FakePortfolioSource(cls.portfolio),
            clock=SequenceClock(
                cls.evaluated_at - timedelta(milliseconds=500),
                cls.evaluated_at,
                cls.evaluated_at + timedelta(milliseconds=500),
            ),
        )

    def test_publication_writes_notification_paper_registration_then_terminal_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_store = LocalSwingOperationalRunStore(root / "operational")
            outbox = LocalSwingDecisionOutbox(root / "outbox")
            ledger = LocalPaperTradeLedger(root / "paper")

            first = publish_swing_operational_run(
                result=self.result,
                run_store=run_store,
                decision_outbox=outbox,
                paper_ledger=ledger,
            )
            second = publish_swing_operational_run(
                result=self.result,
                run_store=run_store,
                decision_outbox=outbox,
                paper_ledger=ledger,
            )

            self.assertEqual(first, second)
            self.assertEqual(first.run_id, self.result.run_id)
            self.assertEqual(
                outbox.get(first.decision_id).notification_id,
                first.notification_id,
            )
            self.assertEqual(
                ledger.get_registration(first.paper_registration_id).registration_id,
                first.paper_registration_id,
            )
            self.assertEqual(run_store.list_records(), (first,))

    def test_single_schedulable_call_executes_and_publishes_complete_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, record = run_and_publish_swing_operation(
                spec=self.spec,
                quote_source=FakeQuoteSource(self.result.quote_batch),
                portfolio_source=FakePortfolioSource(self.portfolio),
                clock=SequenceClock(
                    self.evaluated_at - timedelta(milliseconds=500),
                    self.evaluated_at,
                    self.evaluated_at + timedelta(milliseconds=500),
                ),
                run_store=LocalSwingOperationalRunStore(root / "operational"),
                decision_outbox=LocalSwingDecisionOutbox(root / "outbox"),
                paper_ledger=LocalPaperTradeLedger(root / "paper"),
            )

            self.assertEqual(result.status, SwingOperationalStatus.COMPLETE)
            self.assertEqual(record.run_id, result.run_id)

    def test_publish_requires_side_effect_stores_before_terminal_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_store = LocalSwingOperationalRunStore(Path(directory))
            with self.assertRaises(SwingOperationalStoreError):
                publish_swing_operational_run(
                    result=self.result,
                    run_store=run_store,
                )
            self.assertEqual(run_store.list_records(), ())

    def test_same_spec_cannot_publish_a_different_terminal_result(self) -> None:
        failed = replace(
            self.result,
            status=SwingOperationalStatus.FAILED,
            action=SwingDecisionAction.NO_TRADE,
            failure_codes=(SwingOperationalFailureCode.DECISION_ASSEMBLY_FAILED,),
            decision_package=None,
            paper_registration=None,
        )
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingOperationalRunStore(Path(directory))
            store.publish(failed)
            with self.assertRaises(SwingOperationalStoreError):
                store.publish(self.result)

    def test_store_rejects_tamper_duplicate_keys_and_unsafe_spec_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingOperationalRunStore(Path(directory))
            record = store.publish(self.result)
            path = store.path_for(record.spec_id)
            path.write_bytes(path.read_bytes().replace(b'"status":"COMPLETE"', b'"status":"FAILED"'))
            with self.assertRaises(SwingOperationalStoreError):
                store.get(record.spec_id)
            for value in ("../escape", "A" * 64, "0" * 63, object()):
                with self.subTest(value=value):
                    with self.assertRaises(SwingOperationalStoreError):
                        store.path_for(value)

    def test_cli_lists_and_shows_sanitized_operational_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = LocalSwingOperationalRunStore(root)
            record = store.publish(self.result)

            output = io.StringIO()
            with redirect_stdout(output):
                exit_code = operational_cli(["--root", str(root), "show", "--spec-id", record.spec_id])
            self.assertEqual(exit_code, 0)
            self.assertIn(record.run_id, output.getvalue())
            self.assertNotIn("api_key", output.getvalue())

    def test_gcs_publication_uses_canonical_create_or_verify_object(self) -> None:
        record = operational_record_from_result(self.result)
        writer = FakeStateObjectWriter()

        first = publish_operational_record_to_gcs(
            record=record,
            bucket="india-swing-private",
            writer=writer,
        )
        second = publish_operational_record_to_gcs(
            record=record,
            bucket="india-swing-private",
            writer=writer,
        )

        self.assertEqual(first.publication_id, second.publication_id)
        self.assertEqual(
            first.published_object.object_name,
            operational_record_object_name(record),
        )
        self.assertEqual(writer.calls[0]["content_type"], "application/json")
        self.assertEqual(writer.calls[0]["maximum_bytes"], 512 * 1024)
        first.verify_content_identity()

    def test_gcs_publication_rejects_wrong_path_and_invalid_bucket(self) -> None:
        record = operational_record_from_result(self.result)
        with self.assertRaises(SwingOperationalGCSError):
            publish_operational_record_to_gcs(
                record=record,
                bucket="BAD_BUCKET",
                writer=FakeStateObjectWriter(),
            )
        with self.assertRaises(SwingOperationalGCSError):
            publish_operational_record_to_gcs(
                record=record,
                bucket="india-swing-private",
                writer=FakeStateObjectWriter(wrong_path=True),
            )


if __name__ == "__main__":
    unittest.main()
