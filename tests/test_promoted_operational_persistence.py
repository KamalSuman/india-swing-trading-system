from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from india_swing.market_data.models import FullQuoteBatch
from india_swing.promoted_operational_decision import (
    PAPER_RESEARCH_WARNING,
    PromotedOperationalDecisionAction,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    PromotedOperationalAdvisoryRecord,
    PromotedOperationalPersistenceError,
    PromotedOperationalStoreError,
    build_promoted_operational_advisory,
    build_promoted_operational_terminal_record,
    decode_promoted_operational_advisory,
    decode_promoted_operational_terminal_record,
    encode_promoted_operational_advisory,
    encode_promoted_operational_terminal_record,
    promoted_paper_registration_from_result,
)
from india_swing.promoted_operational_runner import (
    PromotedOperationalRunStatus,
    execute_promoted_operational_run,
)

from tests import test_promoted_operational_runner as _runner_tests


def _vetoed_quote_responder(preparation):
    happy = {
        quote.listing_key: replace(quote, depth_sell=())
        for quote in _runner_tests._QUOTE_GATE_FIXTURE._happy_quotes(preparation)
    }

    def _respond(listing_keys):
        return FullQuoteBatch(
            requested_keys=listing_keys,
            requested_at=_runner_tests._RUN_EVALUATED_AT - timedelta(seconds=3),
            observed_at=_runner_tests._RUN_EVALUATED_AT - timedelta(seconds=1),
            provider_version="kiteconnect/5.2.0",
            quotes=tuple(happy[key] for key in listing_keys),
        )

    return _respond


def _run(spec, quote_source, portfolio_source):
    clock = _runner_tests._clock(
        _runner_tests._STARTED_AT, _runner_tests._RUN_EVALUATED_AT, _runner_tests._COMPLETED_AT
    )
    return execute_promoted_operational_run(
        spec=spec, quote_source=quote_source, portfolio_source=portfolio_source, clock=clock
    )


def _complete_paper_buy_result():
    preparation, spec = _runner_tests._run_spec(chunk_size=500)
    quote_source = _runner_tests._FakeQuoteSource(
        responder=_runner_tests._sorted_quote_responder(preparation)
    )
    return _run(spec, quote_source, _runner_tests._happy_portfolio_source())


def _complete_no_trade_result():
    preparation, spec = _runner_tests._run_spec(chunk_size=250)
    quote_source = _runner_tests._FakeQuoteSource(responder=_vetoed_quote_responder(preparation))
    return _run(spec, quote_source, _runner_tests._happy_portfolio_source())


def _failed_result():
    preparation, spec = _runner_tests._run_spec(chunk_size=100)

    def _boom(_keys):
        raise RuntimeError("boom")

    quote_source = _runner_tests._FakeQuoteSource(responder=_boom)
    return _run(spec, quote_source, _runner_tests._happy_portfolio_source())


def _zero_candidate_complete_result():
    preparation = _runner_tests._QUOTE_GATE_FIXTURE._empty_preparation()
    _, spec = _runner_tests._run_spec(preparation=preparation, chunk_size=500)

    def _explode(_keys):
        raise AssertionError("quote source must not be called for a zero-candidate run")

    quote_source = _runner_tests._FakeQuoteSource(responder=_explode)
    return _run(spec, quote_source, _runner_tests._happy_portfolio_source())


class PromotedOperationalPersistenceTests(unittest.TestCase):
    def test_complete_buy_no_trade_and_failed_advisories_replay_exact_text_hash_lineage_and_authority(
        self,
    ) -> None:
        buy_result = _complete_paper_buy_result()
        buy_advisory = build_promoted_operational_advisory(buy_result)
        self.assertEqual(buy_advisory.status, PromotedOperationalRunStatus.COMPLETE)
        self.assertEqual(buy_advisory.action, PromotedOperationalDecisionAction.PAPER_BUY)
        self.assertEqual(buy_advisory.advisory_text, buy_result.decision_package.advisory_text)
        self.assertEqual(
            buy_advisory.advisory_sha256, buy_result.decision_package.advisory_sha256
        )
        self.assertEqual(
            buy_advisory.decision_id, buy_result.decision_package.decision.decision_id
        )
        self.assertEqual(buy_advisory.package_id, buy_result.decision_package.package_id)
        self.assertEqual(buy_advisory.evaluated_at, buy_result.evaluated_at)
        self.assertEqual(buy_advisory.failure_codes, ())
        self.assertTrue(buy_advisory.paper_only)
        self.assertFalse(buy_advisory.notification_eligible)
        self.assertFalse(buy_advisory.execution_eligible)
        buy_advisory.verify_content_identity()
        self.assertEqual(
            build_promoted_operational_advisory(buy_result).advisory_id, buy_advisory.advisory_id
        )

        no_trade_result = _complete_no_trade_result()
        no_trade_advisory = build_promoted_operational_advisory(no_trade_result)
        self.assertEqual(no_trade_advisory.action, PromotedOperationalDecisionAction.NO_TRADE)
        self.assertEqual(
            no_trade_advisory.advisory_text, no_trade_result.decision_package.advisory_text
        )
        self.assertIsNotNone(no_trade_advisory.decision_id)
        self.assertIsNotNone(no_trade_advisory.package_id)
        no_trade_advisory.verify_content_identity()

        failed_result = _failed_result()
        failed_advisory = build_promoted_operational_advisory(failed_result)
        self.assertEqual(failed_advisory.status, PromotedOperationalRunStatus.FAILED)
        self.assertEqual(failed_advisory.action, PromotedOperationalDecisionAction.NO_TRADE)
        self.assertIsNone(failed_advisory.decision_id)
        self.assertIsNone(failed_advisory.package_id)
        self.assertTrue(failed_advisory.advisory_text.startswith(PAPER_RESEARCH_WARNING + "\n"))
        self.assertIn("Status: FAILED", failed_advisory.advisory_text)
        for code in failed_result.failure_codes:
            self.assertIn(code, failed_advisory.advisory_text)
        failed_advisory.verify_content_identity()
        self.assertEqual(
            build_promoted_operational_advisory(failed_result).advisory_id,
            failed_advisory.advisory_id,
        )

    def test_promoted_buy_maps_exactly_to_existing_paper_registration_without_price_or_quantity_mutation(
        self,
    ) -> None:
        result = _complete_paper_buy_result()
        advisory = build_promoted_operational_advisory(result)
        registration = promoted_paper_registration_from_result(result, advisory)
        self.assertIsNotNone(registration)
        recommendation = result.decision_package.decision.recommendation
        outcome = recommendation.outcome
        evaluation_intent = outcome.quote_outcome.candidate.research_intent.evaluation_intent

        self.assertEqual(registration.symbol, recommendation.symbol)
        self.assertEqual(registration.quantity, recommendation.quantity)
        self.assertEqual(registration.entry_low, outcome.reference_entry_price)
        self.assertEqual(registration.entry_high, evaluation_intent.entry_order.limit_price)
        self.assertEqual(registration.stop, evaluation_intent.stop_price)
        self.assertEqual(registration.target, evaluation_intent.target_price)
        self.assertEqual(
            registration.max_holding_sessions, evaluation_intent.max_holding_sessions
        )
        self.assertEqual(
            registration.estimated_round_trip_cost, outcome.estimated_round_trip_cost
        )
        self.assertEqual(registration.decision_time, result.evaluated_at)
        self.assertEqual(registration.alert_id, advisory.advisory_id)
        self.assertEqual(registration.source_run_id, result.result_id)
        self.assertEqual(
            registration.source_pipeline_integrity_hash,
            result.spec.quote_gate_spec.preparation.manifest.preparation_id,
        )
        self.assertEqual(
            registration.source_decision_integrity_hash,
            result.decision_package.decision.decision_id,
        )
        self.assertEqual(registration.mode, "PAPER_ONLY")
        self.assertFalse(registration.actionable)
        registration.verify_content_identity()

    def test_registration_adapter_returns_none_for_no_trade_and_failed_and_rejects_cross_linked_advisory(
        self,
    ) -> None:
        no_trade_result = _complete_no_trade_result()
        no_trade_advisory = build_promoted_operational_advisory(no_trade_result)
        self.assertIsNone(
            promoted_paper_registration_from_result(no_trade_result, no_trade_advisory)
        )

        failed_result = _failed_result()
        failed_advisory = build_promoted_operational_advisory(failed_result)
        self.assertIsNone(promoted_paper_registration_from_result(failed_result, failed_advisory))

        buy_result = _complete_paper_buy_result()
        with self.assertRaises(PromotedOperationalPersistenceError):
            promoted_paper_registration_from_result(buy_result, no_trade_advisory)

    def test_terminal_record_shapes_cover_buy_no_trade_failed_and_zero_candidate_complete(
        self,
    ) -> None:
        buy_result = _complete_paper_buy_result()
        buy_advisory = build_promoted_operational_advisory(buy_result)
        buy_registration = promoted_paper_registration_from_result(buy_result, buy_advisory)
        buy_terminal = build_promoted_operational_terminal_record(
            buy_result, buy_advisory, buy_registration
        )
        self.assertEqual(buy_terminal.status, PromotedOperationalRunStatus.COMPLETE)
        self.assertEqual(buy_terminal.action, PromotedOperationalDecisionAction.PAPER_BUY)
        self.assertIsNotNone(buy_terminal.paper_registration_id)
        self.assertIsNotNone(buy_terminal.decision_id)
        self.assertIsNotNone(buy_terminal.allocation_batch_id)
        buy_terminal.verify_content_identity()

        no_trade_result = _complete_no_trade_result()
        no_trade_advisory = build_promoted_operational_advisory(no_trade_result)
        no_trade_terminal = build_promoted_operational_terminal_record(
            no_trade_result, no_trade_advisory, None
        )
        self.assertEqual(no_trade_terminal.action, PromotedOperationalDecisionAction.NO_TRADE)
        self.assertIsNone(no_trade_terminal.paper_registration_id)
        self.assertIsNotNone(no_trade_terminal.decision_id)
        no_trade_terminal.verify_content_identity()

        failed_result = _failed_result()
        failed_advisory = build_promoted_operational_advisory(failed_result)
        failed_terminal = build_promoted_operational_terminal_record(
            failed_result, failed_advisory, None
        )
        self.assertEqual(failed_terminal.status, PromotedOperationalRunStatus.FAILED)
        self.assertEqual(failed_terminal.action, PromotedOperationalDecisionAction.NO_TRADE)
        self.assertIsNone(failed_terminal.decision_id)
        self.assertIsNone(failed_terminal.package_id)
        self.assertIsNone(failed_terminal.paper_registration_id)
        self.assertTrue(failed_terminal.failure_codes)
        failed_terminal.verify_content_identity()

        zero_result = _zero_candidate_complete_result()
        zero_advisory = build_promoted_operational_advisory(zero_result)
        zero_terminal = build_promoted_operational_terminal_record(zero_result, zero_advisory, None)
        self.assertIsNone(zero_terminal.quote_batch_id)
        self.assertIsNotNone(zero_terminal.portfolio_context_id)
        self.assertIsNotNone(zero_terminal.quote_gate_batch_id)
        self.assertIsNotNone(zero_terminal.allocation_batch_id)
        self.assertIsNotNone(zero_terminal.decision_id)
        zero_terminal.verify_content_identity()

        with self.assertRaises(PromotedOperationalPersistenceError):
            build_promoted_operational_terminal_record(buy_result, buy_advisory, None)
        with self.assertRaises(PromotedOperationalPersistenceError):
            build_promoted_operational_terminal_record(
                no_trade_result, no_trade_advisory, buy_registration
            )

    def test_advisory_and_terminal_codecs_reject_duplicate_unknown_float_malformed_oversized_noncanonical_and_self_consistent_forgery(
        self,
    ) -> None:
        result = _complete_paper_buy_result()
        advisory = build_promoted_operational_advisory(result)
        registration = promoted_paper_registration_from_result(result, advisory)
        terminal = build_promoted_operational_terminal_record(result, advisory, registration)

        good_advisory_payload = encode_promoted_operational_advisory(advisory)
        good_terminal_payload = encode_promoted_operational_terminal_record(terminal)

        self.assertEqual(
            decode_promoted_operational_advisory(good_advisory_payload).advisory_id,
            advisory.advisory_id,
        )
        self.assertEqual(
            decode_promoted_operational_terminal_record(good_terminal_payload).terminal_id,
            terminal.terminal_id,
        )

        with self.subTest(case="duplicate_keys"):
            malformed = good_advisory_payload.replace(
                b'"paper_only":true', b'"paper_only":true,"paper_only":true', 1
            )
            with self.assertRaises(PromotedOperationalStoreError):
                decode_promoted_operational_advisory(malformed)

        with self.subTest(case="unknown_field"):
            malformed = good_advisory_payload.replace(
                b'"advisory_id"', b'"unexpected_field":true,"advisory_id"', 1
            )
            with self.assertRaises(PromotedOperationalStoreError):
                decode_promoted_operational_advisory(malformed)

        with self.subTest(case="float_literal"):
            malformed = good_terminal_payload.replace(b'"paper_only":true', b'"paper_only":1.5', 1)
            with self.assertRaises(PromotedOperationalStoreError):
                decode_promoted_operational_terminal_record(malformed)

        with self.subTest(case="malformed_json"):
            with self.assertRaises(PromotedOperationalStoreError):
                decode_promoted_operational_advisory(b"{not valid json")

        with self.subTest(case="oversized"):
            huge = b'{"codec_schema_version":"x","advisory":{}}' + b" " * (200 * 1024)
            with self.assertRaises(PromotedOperationalStoreError):
                decode_promoted_operational_advisory(huge)

        with self.subTest(case="noncanonical_encoding"):
            reformatted = json.dumps(
                json.loads(good_advisory_payload), sort_keys=False, indent=2
            ).encode("utf-8")
            with self.assertRaises(PromotedOperationalStoreError):
                decode_promoted_operational_advisory(reformatted)

        with self.subTest(case="self_consistent_forgery_of_failed_text"):
            failed_result = _failed_result()
            failed_advisory = build_promoted_operational_advisory(failed_result)
            forged_text = failed_advisory.advisory_text.replace(
                failed_result.failure_codes[0], "FORGED_CODE"
            )
            forged_sha256 = hashlib.sha256(forged_text.encode("utf-8")).hexdigest()
            with self.assertRaises(PromotedOperationalPersistenceError):
                PromotedOperationalAdvisoryRecord(
                    spec_id=failed_advisory.spec_id,
                    result_id=failed_advisory.result_id,
                    target_session=failed_advisory.target_session,
                    status=failed_advisory.status,
                    action=failed_advisory.action,
                    evaluated_at=failed_advisory.evaluated_at,
                    decision_id=failed_advisory.decision_id,
                    package_id=failed_advisory.package_id,
                    failure_codes=failed_advisory.failure_codes,
                    advisory_text=forged_text,
                    advisory_sha256=forged_sha256,
                    paper_only=True,
                    notification_eligible=False,
                    execution_eligible=False,
                )

    def test_local_stores_are_create_once_idempotent_conflict_detecting_and_reject_links_unsafe_files_and_tampering(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(root)
            terminal_store = LocalPromotedOperationalTerminalStore(root)

            result = _complete_paper_buy_result()
            advisory = build_promoted_operational_advisory(result)
            registration = promoted_paper_registration_from_result(result, advisory)
            terminal = build_promoted_operational_terminal_record(result, advisory, registration)

            first_advisory = advisory_outbox.put(advisory)
            original_advisory_bytes = advisory_outbox.path_for(advisory.advisory_id).read_bytes()
            second_advisory = advisory_outbox.put(advisory)
            self.assertEqual(first_advisory, second_advisory)
            self.assertEqual(
                advisory_outbox.path_for(advisory.advisory_id).read_bytes(),
                original_advisory_bytes,
            )

            first_terminal = terminal_store.put(terminal)
            second_terminal = terminal_store.put(terminal)
            self.assertEqual(first_terminal, second_terminal)

            with self.subTest(case="conflicting_pre_existing_advisory_content"):
                conflict_path = advisory_outbox.path_for(advisory.advisory_id)
                conflict_path.write_bytes(b'{"codec_schema_version":"other","advisory":{}}\n')
                with self.assertRaises(PromotedOperationalStoreError):
                    advisory_outbox.put(advisory)
                # Restore for later subtests.
                conflict_path.write_bytes(original_advisory_bytes)

            with self.subTest(case="conflicting_pre_existing_terminal_content"):
                terminal_conflict_path = terminal_store.path_for(terminal.spec_id)
                original_terminal_bytes = terminal_conflict_path.read_bytes()
                terminal_conflict_path.write_bytes(
                    b'{"codec_schema_version":"other","terminal":{}}\n'
                )
                with self.assertRaises(PromotedOperationalStoreError):
                    terminal_store.put(terminal)
                terminal_conflict_path.write_bytes(original_terminal_bytes)

            with self.subTest(case="unsafe_directory_in_place_of_advisory_file"):
                failed_result = _failed_result()
                failed_advisory = build_promoted_operational_advisory(failed_result)
                unsafe_path = advisory_outbox.path_for(failed_advisory.advisory_id)
                unsafe_path.mkdir(parents=True)
                with self.assertRaises(PromotedOperationalStoreError):
                    advisory_outbox.put(failed_advisory)
                with self.assertRaises(PromotedOperationalStoreError):
                    advisory_outbox.get(failed_advisory.advisory_id)

            with self.subTest(case="tampered_stored_advisory_identity"):
                no_trade_result = _complete_no_trade_result()
                no_trade_advisory = build_promoted_operational_advisory(no_trade_result)
                advisory_outbox.put(no_trade_advisory)
                tampered_path = advisory_outbox.path_for(no_trade_advisory.advisory_id)
                # Directly corrupt the stored bytes' text field content while
                # leaving the stored advisory_id unchanged -- a mismatch the
                # store must catch on the next read.
                corrupted = encode_promoted_operational_advisory(no_trade_advisory)
                corrupted = corrupted.replace(
                    no_trade_advisory.advisory_text.encode("utf-8").splitlines()[0],
                    b"TAMPERED FIRST LINE",
                    1,
                )
                tampered_path.write_bytes(corrupted)
                with self.assertRaises(PromotedOperationalStoreError):
                    advisory_outbox.get(no_trade_advisory.advisory_id)

    def test_get_optional_distinguishes_exact_absence_from_corruption_or_unsafe_path(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            terminal_store = LocalPromotedOperationalTerminalStore(root)

            result = _complete_paper_buy_result()
            spec_id = result.spec.spec_id
            self.assertIsNone(terminal_store.get_optional(spec_id))

            advisory = build_promoted_operational_advisory(result)
            registration = promoted_paper_registration_from_result(result, advisory)
            terminal = build_promoted_operational_terminal_record(result, advisory, registration)
            terminal_store.put(terminal)
            self.assertEqual(terminal_store.get_optional(spec_id).terminal_id, terminal.terminal_id)

            corrupt_spec_id = _zero_candidate_complete_result().spec.spec_id
            path = terminal_store.path_for(corrupt_spec_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"not json at all")
            with self.assertRaises(PromotedOperationalStoreError):
                terminal_store.get_optional(corrupt_spec_id)


if __name__ == "__main__":
    unittest.main()
