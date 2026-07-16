from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from india_swing.execution.costs import FillSide
from india_swing.execution.simulator import (
    ExitReason,
    LimitEntryOrder,
    ProtectiveExitOrder,
    SimulationBar,
    simulate_limit_entry,
    simulate_protective_exit,
)


D = Decimal
SIGNAL = date(2026, 7, 15)
ENTRY = date(2026, 7, 16)


def entry_order(**overrides) -> LimitEntryOrder:
    values = dict(
        symbol="RELIANCE",
        signal_session=SIGNAL,
        first_eligible_session=ENTRY,
        expiry_session=date(2026, 7, 17),
        quantity=10,
        limit_price=D("100"),
        tick_size=D("0.01"),
    )
    values.update(overrides)
    return LimitEntryOrder(**values)


def bar(session: date = ENTRY, **overrides) -> SimulationBar:
    values = dict(
        session=session,
        symbol="RELIANCE",
        open=D("99"),
        high=D("103"),
        low=D("98"),
        close=D("102"),
        volume=100000,
    )
    values.update(overrides)
    return SimulationBar(**values)


def exit_order(**overrides) -> ProtectiveExitOrder:
    values = dict(
        symbol="RELIANCE",
        quantity=10,
        entry_session=ENTRY,
        entry_price=D("100"),
        stop_price=D("95"),
        target_price=D("110"),
        tick_size=D("0.01"),
    )
    values.update(overrides)
    return ProtectiveExitOrder(**values)


class ExecutionSimulatorTests(unittest.TestCase):
    def test_signal_session_can_never_fill_entry(self) -> None:
        self.assertIsNone(
            simulate_limit_entry(
                entry_order(), bar(SIGNAL), slippage_bps=D("10")
            )
        )

    def test_gap_below_limit_fills_from_open_with_adverse_slippage(self) -> None:
        result = simulate_limit_entry(entry_order(), bar(), slippage_bps=D("10"))

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIs(result.side, FillSide.BUY)
        self.assertEqual(result.trigger_price, D("99"))
        self.assertEqual(result.fill_price, D("99.10"))

    def test_touched_buy_limit_never_fills_above_limit(self) -> None:
        result = simulate_limit_entry(
            entry_order(),
            bar(open=D("102"), low=D("99")),
            slippage_bps=D("25"),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.trigger_price, D("100"))
        self.assertEqual(result.fill_price, D("100"))

    def test_tick_size_is_explicit_and_slippage_rounds_against_strategy(self) -> None:
        result = simulate_limit_entry(
            entry_order(tick_size=D("0.05")),
            bar(open=D("99.05"), high=D("103.05"), low=D("98.05"), close=D("102.05")),
            slippage_bps=D("10"),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.fill_price, D("99.15"))
        with self.assertRaisesRegex(ValueError, "tick multiple"):
            entry_order(limit_price=D("100.03"), tick_size=D("0.05"))

    def test_entry_requires_limit_touch_liquidity_and_tradability(self) -> None:
        cases = (
            bar(open=D("102"), low=D("101")),
            bar(volume=100),
            bar(open=D("99"), high=D("99"), low=D("99"), close=D("99"), volume=0, tradable=False),
        )
        for simulation_bar in cases:
            with self.subTest(bar_id=simulation_bar.bar_id):
                self.assertIsNone(
                    simulate_limit_entry(
                        entry_order(), simulation_bar, slippage_bps=D("10")
                    )
                )

    def test_gap_through_stop_uses_open_then_adverse_slippage(self) -> None:
        result = simulate_protective_exit(
            exit_order(),
            bar(date(2026, 7, 17), open=D("90"), high=D("93"), low=D("88"), close=D("91")),
            slippage_bps=D("20"),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIs(result.exit_reason, ExitReason.STOP)
        self.assertEqual(result.trigger_price, D("90"))
        self.assertEqual(result.fill_price, D("89.82"))

    def test_stop_wins_when_daily_bar_touches_stop_and_target(self) -> None:
        result = simulate_protective_exit(
            exit_order(),
            bar(date(2026, 7, 17), open=D("100"), high=D("112"), low=D("94"), close=D("105")),
            slippage_bps=D("10"),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIs(result.exit_reason, ExitReason.STOP)
        self.assertEqual(result.fill_price, D("94.90"))

    def test_gapped_target_respects_sell_limit(self) -> None:
        result = simulate_protective_exit(
            exit_order(),
            bar(date(2026, 7, 17), open=D("112"), high=D("114"), low=D("111"), close=D("113")),
            slippage_bps=D("20"),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIs(result.exit_reason, ExitReason.TARGET)
        self.assertEqual(result.trigger_price, D("112"))
        self.assertEqual(result.fill_price, D("111.77"))

    def test_lower_circuit_lock_does_not_assume_stop_fill(self) -> None:
        locked = bar(
            date(2026, 7, 17),
            open=D("90"),
            high=D("90"),
            low=D("90"),
            close=D("90"),
            lower_circuit_sell_locked=True,
        )

        self.assertIsNone(
            simulate_protective_exit(exit_order(), locked, slippage_bps=D("10"))
        )

    def test_entry_session_stop_is_assumed_but_target_only_is_not(self) -> None:
        both_touched = simulate_protective_exit(
            exit_order(),
            bar(open=D("100"), high=D("112"), low=D("94"), close=D("105")),
            slippage_bps=D("10"),
        )
        target_only = simulate_protective_exit(
            exit_order(),
            bar(open=D("100"), high=D("112"), low=D("99"), close=D("105")),
            slippage_bps=D("10"),
        )

        self.assertIsNotNone(both_touched)
        assert both_touched is not None
        self.assertIs(both_touched.exit_reason, ExitReason.STOP)
        self.assertEqual(both_touched.trigger_price, D("95"))
        self.assertIsNone(target_only)

    def test_exit_also_respects_participation_limit(self) -> None:
        thin_bar = bar(
            date(2026, 7, 17),
            open=D("94"),
            high=D("96"),
            low=D("93"),
            close=D("94"),
            volume=100,
        )

        self.assertIsNone(
            simulate_protective_exit(
                exit_order(), thin_bar, slippage_bps=D("10")
            )
        )


if __name__ == "__main__":
    unittest.main()
