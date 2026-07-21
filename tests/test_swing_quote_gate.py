from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from india_swing.market_data.models import FullQuoteBatch, KiteDepthLevel, KiteFullQuote
from india_swing.signals.proposal_batch import assemble_swing_proposal_batch
from india_swing.signals.quote_gate import (
    SwingQuoteGateBatch,
    SwingQuoteGateDisposition,
    SwingQuoteGateError,
    SwingQuoteGateOutcome,
    SwingQuoteGatePolicy,
    SwingQuoteGateReason,
    assemble_swing_quote_gate_batch,
)
from india_swing.signals.universe_batch import assemble_universe_input_batch

from tests.test_swing_proposal_batch import (
    HISTORY_SESSIONS,
    INSTRUMENT_A,
    INSTRUMENT_B,
    LISTING_A,
    LISTING_B,
    TICK_EVIDENCE_A,
    TICK_EVIDENCE_B,
    _build_assembly,
    _calendar,
    _config,
    _current_universe,
    _hex_id,
)


TOKEN_A = 1001
TOKEN_B = 1002


def _depth(price: str, quantity: int = 10, orders: int = 2) -> KiteDepthLevel:
    return KiteDepthLevel(price=Decimal(price), quantity=quantity, orders=orders)


def _quote(
    *,
    listing_key: str,
    instrument_token: int,
    last_price: Decimal,
    lower_circuit: Decimal,
    upper_circuit: Decimal,
    exchange_timestamp,
    last_trade_time,
    best_bid: Decimal | None,
    best_ask: Decimal | None,
) -> KiteFullQuote:
    return KiteFullQuote(
        listing_key=listing_key,
        instrument_token=instrument_token,
        exchange_timestamp=exchange_timestamp,
        last_trade_time=last_trade_time,
        last_price=last_price,
        lower_circuit_limit=lower_circuit,
        upper_circuit_limit=upper_circuit,
        depth_buy=(_depth(str(best_bid)),) if best_bid is not None else (),
        depth_sell=(_depth(str(best_ask)),) if best_ask is not None else (),
    )


class SwingQuoteGatePolicyTests(unittest.TestCase):
    def test_default_policy_is_deterministic_and_content_addressed(self) -> None:
        first = SwingQuoteGatePolicy()
        second = SwingQuoteGatePolicy()

        self.assertEqual(first.policy_id, second.policy_id)
        self.assertEqual(first.maximum_batch_collection_seconds, 15)
        self.assertEqual(first.maximum_quote_age_seconds, 15)
        self.assertEqual(first.maximum_last_trade_age_seconds, 300)
        self.assertEqual(first.maximum_spread_bps, Decimal("50"))
        first.verify_content_identity()

    def test_policy_rejects_bool_as_int_and_non_finite_or_nonpositive_values(self) -> None:
        cases = (
            dict(maximum_batch_collection_seconds=True),
            dict(maximum_quote_age_seconds=True),
            dict(maximum_last_trade_age_seconds=True),
            dict(maximum_batch_collection_seconds=0),
            dict(maximum_quote_age_seconds=-1),
            dict(maximum_spread_bps=Decimal("0")),
            dict(maximum_spread_bps=Decimal("-5")),
            dict(maximum_spread_bps=Decimal("NaN")),
            dict(maximum_spread_bps=50),
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(SwingQuoteGateError):
                    SwingQuoteGatePolicy(**overrides)

    def test_policy_detects_post_construction_mutation(self) -> None:
        policy = SwingQuoteGatePolicy()
        original_id = policy.policy_id
        object.__setattr__(policy, "maximum_spread_bps", Decimal("999"))

        self.assertEqual(policy.policy_id, original_id)
        with self.assertRaisesRegex(SwingQuoteGateError, "content identity"):
            policy.verify_content_identity()


class SwingQuoteGateBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = _config()
        self.calendar = _calendar()
        self.current_universe = _current_universe(
            calendar_snapshot_id=self.calendar.snapshot_id
        )

        raw_ids = tuple(_hex_id(0xA1, index) for index in range(HISTORY_SESSIONS))
        universe_ids = tuple(
            _hex_id(0xB1, index) for index in range(HISTORY_SESSIONS - 1)
        ) + (self.current_universe.snapshot_id,)

        assembly_a = _build_assembly(
            prefix=1,
            instrument_id=INSTRUMENT_A,
            listing_id=LISTING_A,
            symbol="STOCKA",
            raw_ids=raw_ids,
            universe_ids=universe_ids,
            tick_size=Decimal("0.05"),
            tick_evidence_id=TICK_EVIDENCE_A,
            base_close=Decimal("100"),
        )
        assembly_b = _build_assembly(
            prefix=2,
            instrument_id=INSTRUMENT_B,
            listing_id=LISTING_B,
            symbol="STOCKB",
            raw_ids=raw_ids,
            universe_ids=universe_ids,
            tick_size=Decimal("0.10"),
            tick_evidence_id=TICK_EVIDENCE_B,
            base_close=Decimal("500"),
        )
        self.assembly_a = assembly_a
        self.assembly_b = assembly_b
        self.universe_batch = assemble_universe_input_batch(
            current_universe=self.current_universe,
            assemblies=(assembly_a, assembly_b),
        )
        self.proposal_batch = assemble_swing_proposal_batch(
            universe_batch=self.universe_batch, calendar=self.calendar, config=self.config
        )
        self.proposal_a, self.proposal_b = self.proposal_batch.proposals
        self.evaluated_at = self.proposal_a.entry_window.earliest_entry_at + timedelta(
            seconds=2
        )

    def _happy_quote(
        self,
        proposal,
        *,
        instrument_token: int,
        spread_bps: str = "0",
        evaluated_at=None,
    ) -> KiteFullQuote:
        levels = proposal.levels
        evaluated_at = evaluated_at or self.evaluated_at
        exchange_timestamp = evaluated_at - timedelta(seconds=2)
        last_trade_time = evaluated_at - timedelta(seconds=2)
        best_bid = levels.entry_low
        best_ask = best_bid + (best_bid * Decimal(spread_bps) / Decimal("10000"))
        return _quote(
            listing_key=f"NSE:{proposal.symbol}",
            instrument_token=instrument_token,
            last_price=levels.entry_low,
            lower_circuit=levels.entry_low - Decimal("50"),
            upper_circuit=levels.entry_high + Decimal("50"),
            exchange_timestamp=exchange_timestamp,
            last_trade_time=last_trade_time,
            best_bid=best_bid,
            best_ask=best_ask,
        )

    def _happy_batch(self, quote_a=None, quote_b=None, evaluated_at=None) -> FullQuoteBatch:
        evaluated_at = evaluated_at or self.evaluated_at
        quote_a = quote_a or self._happy_quote(
            self.proposal_a,
            instrument_token=TOKEN_A,
            evaluated_at=evaluated_at,
        )
        quote_b = quote_b or self._happy_quote(
            self.proposal_b,
            instrument_token=TOKEN_B,
            evaluated_at=evaluated_at,
        )
        return FullQuoteBatch(
            requested_keys=(f"NSE:{self.proposal_a.symbol}", f"NSE:{self.proposal_b.symbol}"),
            requested_at=evaluated_at - timedelta(seconds=3),
            observed_at=evaluated_at - timedelta(seconds=1),
            provider_version="kiteconnect/5.2.0",
            quotes=(quote_a, quote_b),
        )

    def test_happy_path_produces_two_pass_outcomes(self) -> None:
        quote_batch = self._happy_batch()

        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=self.evaluated_at,
        )

        self.assertEqual(gate_batch.pass_count, 2)
        self.assertEqual(gate_batch.veto_count, 0)
        for outcome, proposal in zip(gate_batch.outcomes, self.proposal_batch.proposals):
            self.assertTrue(outcome.passed)
            self.assertEqual(outcome.reason_codes, ())
            self.assertEqual(outcome.effective_cost_bps, self.config.base_round_trip_cost_bps)
            self.assertIsNotNone(outcome.quote_adjusted_levels)
            self.assertGreaterEqual(
                outcome.quote_adjusted_levels.net_reward_risk, self.config.target_net_reward_risk
            )
            for value in (
                outcome.quote_adjusted_levels.entry_low,
                outcome.quote_adjusted_levels.entry_high,
                outcome.quote_adjusted_levels.stop,
                outcome.quote_adjusted_levels.target,
            ):
                self.assertEqual(
                    value % proposal.assembly.signal_materialization.history.tick_size,
                    Decimal("0"),
                )
            self.assertTrue(outcome.research_only)
            self.assertFalse(outcome.execution_eligible)
        self.assertFalse(gate_batch.execution_eligible)
        gate_batch.verify_content_identity()

    def test_batch_ids_are_deterministic(self) -> None:
        quote_batch = self._happy_batch()
        first = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=self.evaluated_at,
        )
        second = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=self.evaluated_at,
        )

        self.assertEqual(first.gate_batch_id, second.gate_batch_id)

    def test_spread_between_base_and_policy_max_raises_only_the_adjusted_target(self) -> None:
        wide_batch = self._wide_proposal_batch()
        wide_a, wide_b = wide_batch.proposals
        policy = SwingQuoteGatePolicy(maximum_spread_bps=Decimal("50"))
        quote_a = self._happy_quote(wide_a, instrument_token=TOKEN_A, spread_bps="40")
        quote_b = self._happy_quote(wide_b, instrument_token=TOKEN_B)
        quote_batch = FullQuoteBatch(
            requested_keys=(f"NSE:{wide_a.symbol}", f"NSE:{wide_b.symbol}"),
            requested_at=self.evaluated_at - timedelta(seconds=3),
            observed_at=self.evaluated_at - timedelta(seconds=1),
            provider_version="kiteconnect/5.2.0",
            quotes=(quote_a, quote_b),
        )

        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=wide_batch,
            quote_batch=quote_batch,
            evaluated_at=self.evaluated_at,
            policy=policy,
        )

        outcome_a = gate_batch.outcomes[0]
        self.assertTrue(outcome_a.passed)
        self.assertGreater(outcome_a.observed_spread_bps, self.config.base_round_trip_cost_bps)
        self.assertEqual(outcome_a.effective_cost_bps, outcome_a.observed_spread_bps)
        self.assertEqual(
            outcome_a.quote_adjusted_levels.entry_low, wide_a.levels.entry_low
        )
        self.assertEqual(
            outcome_a.quote_adjusted_levels.entry_high, wide_a.levels.entry_high
        )
        self.assertGreater(
            outcome_a.quote_adjusted_levels.target, wide_a.levels.target
        )
        # EOD metrics/base levels are untouched by the quote gate.
        self.assertEqual(
            wide_a.levels.estimated_cost_bps,
            wide_batch.config.base_round_trip_cost_bps,
        )

    def test_each_veto_reason_fires_independently(self) -> None:
        window = self.proposal_a.entry_window
        levels = self.proposal_a.levels
        base_quote = self._happy_quote(self.proposal_a, instrument_token=TOKEN_A)

        cases = {
            SwingQuoteGateReason.ENTRY_WINDOW_NOT_OPEN.value: (
                window.earliest_entry_at - timedelta(seconds=1),
                self._happy_quote(
                    self.proposal_a,
                    instrument_token=TOKEN_A,
                    evaluated_at=window.earliest_entry_at - timedelta(seconds=1),
                ),
            ),
            SwingQuoteGateReason.ENTRY_WINDOW_EXPIRED.value: (
                window.entry_expires_at + timedelta(seconds=1),
                replace(
                    base_quote,
                    exchange_timestamp=window.entry_expires_at,
                    last_trade_time=window.entry_expires_at,
                ),
            ),
            SwingQuoteGateReason.QUOTE_OUTSIDE_ENTRY_WINDOW.value: (
                self.evaluated_at,
                replace(
                    base_quote,
                    exchange_timestamp=window.earliest_entry_at - timedelta(seconds=1),
                    last_trade_time=window.earliest_entry_at - timedelta(seconds=1),
                ),
            ),
            SwingQuoteGateReason.QUOTE_STALE.value: (
                self.evaluated_at,
                replace(
                    base_quote,
                    exchange_timestamp=self.evaluated_at - timedelta(seconds=20),
                    last_trade_time=self.evaluated_at - timedelta(seconds=20),
                ),
            ),
            SwingQuoteGateReason.LAST_TRADE_TIME_MISSING.value: (
                self.evaluated_at,
                replace(base_quote, last_trade_time=None),
            ),
            SwingQuoteGateReason.LAST_TRADE_OUTSIDE_ENTRY_WINDOW.value: (
                self.evaluated_at,
                replace(base_quote, last_trade_time=window.earliest_entry_at - timedelta(seconds=1)),
            ),
            SwingQuoteGateReason.LAST_TRADE_STALE.value: (
                self.evaluated_at,
                replace(base_quote, last_trade_time=self.evaluated_at - timedelta(seconds=400)),
            ),
            SwingQuoteGateReason.TWO_SIDED_DEPTH_MISSING.value: (
                self.evaluated_at,
                replace(base_quote, depth_sell=()),
            ),
            SwingQuoteGateReason.CIRCUIT_LOCKED.value: (
                self.evaluated_at,
                replace(base_quote, lower_circuit_limit=levels.entry_low),
            ),
            SwingQuoteGateReason.LAST_PRICE_OUTSIDE_ENTRY_RANGE.value: (
                self.evaluated_at,
                replace(
                    base_quote,
                    last_price=levels.entry_high + Decimal("100"),
                    upper_circuit_limit=levels.entry_high + Decimal("200"),
                ),
            ),
            SwingQuoteGateReason.BEST_ASK_OUTSIDE_ENTRY_RANGE.value: (
                self.evaluated_at,
                replace(
                    base_quote,
                    depth_sell=(_depth(str(levels.entry_high + Decimal("100"))),),
                ),
            ),
        }
        for reason, (evaluated_at, quote) in cases.items():
            with self.subTest(reason=reason):
                quote_batch = self._happy_batch(quote_a=quote, evaluated_at=evaluated_at)
                gate_batch = assemble_swing_quote_gate_batch(
                    proposal_batch=self.proposal_batch,
                    quote_batch=quote_batch,
                    evaluated_at=evaluated_at,
                )
                outcome_a = gate_batch.outcomes[0]
                self.assertFalse(outcome_a.passed)
                self.assertIn(reason, outcome_a.reason_codes)
                self.assertIsNone(outcome_a.effective_cost_bps)
                self.assertIsNone(outcome_a.quote_adjusted_levels)

    def test_spread_above_policy_max_is_vetoed(self) -> None:
        base_quote = self._happy_quote(self.proposal_a, instrument_token=TOKEN_A)
        wide_quote = replace(
            base_quote,
            depth_sell=(_depth(str(self.proposal_a.levels.entry_low + Decimal("50"))),),
        )
        quote_batch = self._happy_batch(quote_a=wide_quote)

        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=self.evaluated_at,
        )

        outcome_a = gate_batch.outcomes[0]
        self.assertFalse(outcome_a.passed)
        self.assertIn(SwingQuoteGateReason.SPREAD_ABOVE_POLICY_MAX.value, outcome_a.reason_codes)

    def test_multiple_simultaneous_veto_conditions_accumulate_deterministically(self) -> None:
        window = self.proposal_a.entry_window
        base_quote = self._happy_quote(self.proposal_a, instrument_token=TOKEN_A)
        bad_quote = replace(
            base_quote,
            exchange_timestamp=window.earliest_entry_at - timedelta(seconds=3),
            last_trade_time=None,
            depth_sell=(),
        )

        evaluated_at = window.earliest_entry_at - timedelta(seconds=1)
        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=self._happy_batch(quote_a=bad_quote, evaluated_at=evaluated_at),
            evaluated_at=evaluated_at,
        )

        outcome_a = gate_batch.outcomes[0]
        self.assertFalse(outcome_a.passed)
        self.assertIn(SwingQuoteGateReason.ENTRY_WINDOW_NOT_OPEN.value, outcome_a.reason_codes)
        self.assertIn(SwingQuoteGateReason.LAST_TRADE_TIME_MISSING.value, outcome_a.reason_codes)
        self.assertIn(SwingQuoteGateReason.TWO_SIDED_DEPTH_MISSING.value, outcome_a.reason_codes)
        self.assertIn(SwingQuoteGateReason.SPREAD_UNAVAILABLE.value, outcome_a.reason_codes)
        self.assertEqual(outcome_a.reason_codes, tuple(sorted(set(outcome_a.reason_codes))))

    def _wide_proposal_batch(self):
        wide_config = replace(self.config, entry_atr_buffer=Decimal("2.0"))
        return assemble_swing_proposal_batch(
            universe_batch=self.universe_batch, calendar=self.calendar, config=wide_config
        )

    def test_boundary_ages_and_spread_are_inclusive(self) -> None:
        wide_batch = self._wide_proposal_batch()
        wide_a, wide_b = wide_batch.proposals
        evaluated_at = wide_a.entry_window.earliest_entry_at + timedelta(seconds=300)
        levels = wide_a.levels
        best_bid = levels.entry_low
        best_ask = best_bid + best_bid * Decimal("50") / Decimal("10000")
        quote_a = _quote(
            listing_key=f"NSE:{wide_a.symbol}",
            instrument_token=TOKEN_A,
            last_price=levels.entry_low,
            lower_circuit=levels.entry_low - Decimal("50"),
            upper_circuit=levels.entry_high + Decimal("50"),
            exchange_timestamp=evaluated_at - timedelta(seconds=15),
            last_trade_time=evaluated_at - timedelta(seconds=300),
            best_bid=best_bid,
            best_ask=best_ask,
        )
        levels_b = wide_b.levels
        quote_b = _quote(
            listing_key=f"NSE:{wide_b.symbol}",
            instrument_token=TOKEN_B,
            last_price=levels_b.entry_low,
            lower_circuit=levels_b.entry_low - Decimal("50"),
            upper_circuit=levels_b.entry_high + Decimal("50"),
            exchange_timestamp=evaluated_at - timedelta(seconds=2),
            last_trade_time=evaluated_at - timedelta(seconds=2),
            best_bid=levels_b.entry_low,
            best_ask=levels_b.entry_low,
        )
        quote_batch = FullQuoteBatch(
            requested_keys=(f"NSE:{wide_a.symbol}", f"NSE:{wide_b.symbol}"),
            requested_at=evaluated_at - timedelta(seconds=3),
            observed_at=evaluated_at - timedelta(seconds=1),
            provider_version="kiteconnect/5.2.0",
            quotes=(quote_a, quote_b),
        )

        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=wide_batch,
            quote_batch=quote_batch,
            evaluated_at=evaluated_at,
        )

        self.assertTrue(gate_batch.outcomes[0].passed)

    def test_rejects_key_mismatch_missing_extra_or_duplicate(self) -> None:
        quote_batch = self._happy_batch()

        with self.assertRaises(SwingQuoteGateError):
            assemble_swing_quote_gate_batch(
                proposal_batch=self.proposal_batch,
                quote_batch=replace(
                    quote_batch,
                    requested_keys=(quote_batch.requested_keys[0],),
                    quotes=(quote_batch.quotes[0],),
                ),
                evaluated_at=self.evaluated_at,
            )

    def test_rejects_evaluation_before_observed_at_and_excessive_collection_duration(
        self,
    ) -> None:
        quote_batch = self._happy_batch()

        with self.assertRaises(SwingQuoteGateError):
            assemble_swing_quote_gate_batch(
                proposal_batch=self.proposal_batch,
                quote_batch=quote_batch,
                evaluated_at=quote_batch.observed_at - timedelta(seconds=1),
            )

        slow_quote_batch = replace(
            quote_batch, requested_at=quote_batch.observed_at - timedelta(seconds=30)
        )
        with self.assertRaises(SwingQuoteGateError):
            assemble_swing_quote_gate_batch(
                proposal_batch=self.proposal_batch,
                quote_batch=slow_quote_batch,
                evaluated_at=self.evaluated_at,
            )

    def test_rejects_wrong_exact_types(self) -> None:
        quote_batch = self._happy_batch()
        with self.assertRaises(SwingQuoteGateError):
            assemble_swing_quote_gate_batch(
                proposal_batch=object(),
                quote_batch=quote_batch,
                evaluated_at=self.evaluated_at,
            )
        with self.assertRaises(SwingQuoteGateError):
            assemble_swing_quote_gate_batch(
                proposal_batch=self.proposal_batch,
                quote_batch=object(),
                evaluated_at=self.evaluated_at,
            )
        with self.assertRaises(SwingQuoteGateError):
            assemble_swing_quote_gate_batch(
                proposal_batch=self.proposal_batch,
                quote_batch=quote_batch,
                evaluated_at=self.evaluated_at,
                policy=object(),
            )

    def test_direct_construction_rejects_missing_extra_duplicate_or_reordered_outcomes(
        self,
    ) -> None:
        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=self._happy_batch(),
            evaluated_at=self.evaluated_at,
        )
        outcome_a, outcome_b = gate_batch.outcomes

        with self.assertRaises(SwingQuoteGateError):
            replace(gate_batch, outcomes=(outcome_a,), pass_count=1)
        with self.assertRaises(SwingQuoteGateError):
            replace(gate_batch, outcomes=(outcome_a, outcome_a), pass_count=2)
        with self.assertRaises(SwingQuoteGateError):
            replace(gate_batch, outcomes=(outcome_b, outcome_a))

    def test_direct_construction_cannot_forge_pass_veto_fields_or_counts(self) -> None:
        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=self._happy_batch(),
            evaluated_at=self.evaluated_at,
        )
        outcome_a = gate_batch.outcomes[0]

        with self.assertRaises(SwingQuoteGateError):
            replace(outcome_a, disposition=SwingQuoteGateDisposition.PASS, reason_codes=("FORGED",))
        with self.assertRaises(SwingQuoteGateError):
            replace(gate_batch, pass_count=gate_batch.pass_count + 1)
        with self.assertRaises(SwingQuoteGateError):
            replace(gate_batch, veto_count=gate_batch.veto_count + 5)

    def test_direct_construction_cannot_forge_cost_or_levels(self) -> None:
        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=self._happy_batch(),
            evaluated_at=self.evaluated_at,
        )
        outcome_a = gate_batch.outcomes[0]

        with self.assertRaisesRegex(SwingQuoteGateError, "does not replay"):
            replace(
                outcome_a,
                effective_cost_bps=outcome_a.effective_cost_bps + Decimal("100"),
            )

    def test_verify_content_identity_detects_nested_mutation_without_disturbing_gate_batch_id(
        self,
    ) -> None:
        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=self._happy_batch(),
            evaluated_at=self.evaluated_at,
        )
        original_id = gate_batch.gate_batch_id
        object.__setattr__(
            gate_batch.outcomes[0].quote.depth_buy[0], "price", Decimal("1")
        )

        self.assertEqual(gate_batch.gate_batch_id, original_id)
        with self.assertRaises(Exception):
            gate_batch.verify_content_identity()

    def test_verify_content_identity_detects_policy_and_outcome_mutation(self) -> None:
        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.proposal_batch,
            quote_batch=self._happy_batch(),
            evaluated_at=self.evaluated_at,
        )
        original_id = gate_batch.gate_batch_id
        object.__setattr__(gate_batch.policy, "maximum_spread_bps", Decimal("1"))

        self.assertEqual(gate_batch.gate_batch_id, original_id)
        with self.assertRaises(Exception):
            gate_batch.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
