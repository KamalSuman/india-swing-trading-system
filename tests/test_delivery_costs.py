from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date
from decimal import Decimal

from india_swing.execution.costs import (
    CostScheduleError,
    DeliveryFill,
    FillSide,
    ZerodhaDpTariff,
    calculate_delivery_charges,
    calculate_equity_cash_charges,
    zerodha_nse_delivery_schedule_2026,
)


D = Decimal
ISIN_A = "INE002A01018"
ISIN_B = "INE009A01021"


def fill(
    trade_date: date,
    side: FillSide,
    quantity: int,
    price: str,
    order_id: str,
    *,
    symbol: str = "RELIANCE",
    isin: str = ISIN_A,
) -> DeliveryFill:
    return DeliveryFill(
        trade_date=trade_date,
        symbol=symbol,
        isin=isin,
        side=side,
        quantity=quantity,
        price=D(price),
        order_id=order_id,
    )


class DeliveryCostTests(unittest.TestCase):
    def test_itemized_round_trip_uses_contract_day_rules(self) -> None:
        schedule = zerodha_nse_delivery_schedule_2026()
        charges = calculate_delivery_charges(
            (
                fill(date(2026, 7, 16), FillSide.BUY, 1000, "100", "buy-1"),
                fill(date(2026, 7, 20), FillSide.SELL, 1000, "110", "sell-1"),
            ),
            schedule,
        )

        buy, sell = charges.legs
        self.assertEqual(buy.turnover, D("100000.00"))
        self.assertEqual(buy.brokerage, D("0.00"))
        self.assertEqual(buy.stt, D("100"))
        self.assertEqual(buy.exchange_and_ipft, D("3.07"))
        self.assertEqual(buy.sebi, D("0.10"))
        self.assertEqual(buy.stamp, D("15"))
        self.assertEqual(buy.gst, D("0.57"))
        self.assertEqual(buy.dp_total, D("0.00"))
        self.assertEqual(buy.total, D("118.74"))

        self.assertEqual(sell.turnover, D("110000.00"))
        self.assertEqual(sell.stt, D("110"))
        self.assertEqual(sell.exchange_and_ipft, D("3.38"))
        self.assertEqual(sell.sebi, D("0.11"))
        self.assertEqual(sell.stamp, D("0"))
        self.assertEqual(sell.gst, D("0.63"))
        self.assertEqual(sell.dp_base, D("13.00"))
        self.assertEqual(sell.dp_gst, D("2.34"))
        self.assertEqual(sell.total, D("129.46"))
        self.assertEqual(charges.total, D("248.20"))
        self.assertEqual(len(charges.schedule_id), 64)
        self.assertEqual(len(charges.calculation_id), 64)

    def test_dp_is_once_per_sold_scrip_per_contract_day(self) -> None:
        day = date(2026, 7, 16)
        charges = calculate_delivery_charges(
            (
                fill(day, FillSide.SELL, 20, "100", "sell-a1"),
                fill(day, FillSide.SELL, 30, "100", "sell-a2"),
                fill(
                    day,
                    FillSide.SELL,
                    10,
                    "200",
                    "sell-b",
                    symbol="INFY",
                    isin=ISIN_B,
                ),
            ),
            zerodha_nse_delivery_schedule_2026(),
        )

        self.assertEqual(charges.legs[0].dp_base, D("26.00"))
        self.assertEqual(charges.legs[0].dp_gst, D("4.68"))

    def test_female_first_holder_discount_requires_explicit_tariff(self) -> None:
        standard = zerodha_nse_delivery_schedule_2026()
        discounted = zerodha_nse_delivery_schedule_2026(
            dp_tariff=ZerodhaDpTariff.RESIDENT_RETAIL_FEMALE_FIRST_HOLDER
        )
        sale = (fill(date(2026, 7, 16), FillSide.SELL, 1, "100", "sell"),)

        standard_dp = calculate_delivery_charges(sale, standard).legs[0].dp_total
        discounted_dp = calculate_delivery_charges(sale, discounted).legs[0].dp_total

        self.assertEqual(standard_dp, D("15.34"))
        self.assertEqual(discounted_dp, D("15.05"))
        self.assertNotEqual(standard.schedule_id, discounted.schedule_id)

    def test_schedule_fails_closed_outside_effective_dates(self) -> None:
        schedule = zerodha_nse_delivery_schedule_2026()
        historical = (fill(date(2026, 2, 28), FillSide.BUY, 1, "100", "buy"),)

        with self.assertRaisesRegex(CostScheduleError, "does not apply"):
            calculate_delivery_charges(historical, schedule)

    def test_exchange_rate_already_includes_ipft(self) -> None:
        schedule = zerodha_nse_delivery_schedule_2026()
        self.assertEqual(schedule.exchange_and_ipft_bps, D("0.307"))
        self.assertFalse(hasattr(schedule, "ipft_bps"))

    def test_duplicate_fill_identity_is_rejected(self) -> None:
        one_fill = fill(date(2026, 7, 16), FillSide.BUY, 1, "100", "buy")

        with self.assertRaisesRegex(CostScheduleError, "duplicate"):
            calculate_delivery_charges(
                (one_fill, one_fill), zerodha_nse_delivery_schedule_2026()
            )

    def test_same_day_round_trip_is_not_mispriced_as_delivery(self) -> None:
        day = date(2026, 7, 16)
        with self.assertRaisesRegex(CostScheduleError, "ambiguous"):
            calculate_delivery_charges(
                (
                    fill(day, FillSide.BUY, 1, "100", "buy"),
                    fill(day, FillSide.SELL, 1, "101", "sell"),
                ),
                zerodha_nse_delivery_schedule_2026(),
            )

    def test_same_day_round_trip_uses_intraday_rates_and_no_dp(self) -> None:
        day = date(2026, 7, 16)
        charges = calculate_equity_cash_charges(
            (
                fill(day, FillSide.BUY, 100, "99.10", "buy"),
                fill(day, FillSide.SELL, 100, "94.90", "sell"),
            ),
            zerodha_nse_delivery_schedule_2026(),
        )

        leg = charges.legs[0]
        self.assertEqual(leg.brokerage, D("5.82"))
        self.assertEqual(leg.stt, D("2"))
        self.assertEqual(leg.exchange_and_ipft, D("0.60"))
        self.assertEqual(leg.sebi, D("0.02"))
        self.assertEqual(leg.stamp, D("0"))
        self.assertEqual(leg.gst, D("1.16"))
        self.assertEqual(leg.dp_total, D("0.00"))
        self.assertEqual(leg.total, D("9.60"))

    def test_cash_router_preserves_delivery_pricing_across_dates(self) -> None:
        fills = (
            fill(date(2026, 7, 16), FillSide.BUY, 1000, "100", "buy"),
            fill(date(2026, 7, 20), FillSide.SELL, 1000, "110", "sell"),
        )
        schedule = zerodha_nse_delivery_schedule_2026()

        self.assertEqual(
            calculate_equity_cash_charges(fills, schedule),
            calculate_delivery_charges(fills, schedule),
        )

    def test_intraday_brokerage_cap_applies_per_executed_order(self) -> None:
        day = date(2026, 7, 16)
        charges = calculate_equity_cash_charges(
            (
                fill(day, FillSide.BUY, 10000, "100", "buy"),
                fill(day, FillSide.SELL, 10000, "101", "sell"),
            ),
            zerodha_nse_delivery_schedule_2026(),
        )

        self.assertEqual(charges.legs[0].brokerage, D("40.00"))

    def test_partial_same_day_netting_fails_without_allocation_evidence(self) -> None:
        day = date(2026, 7, 16)
        with self.assertRaisesRegex(CostScheduleError, "allocation evidence"):
            calculate_equity_cash_charges(
                (
                    fill(day, FillSide.BUY, 100, "100", "buy"),
                    fill(day, FillSide.SELL, 60, "101", "sell"),
                ),
                zerodha_nse_delivery_schedule_2026(),
            )

    def test_schedule_identity_changes_with_a_rate(self) -> None:
        schedule = zerodha_nse_delivery_schedule_2026()
        changed = replace(schedule, stamp_buy_bps=D("1.6"))

        self.assertNotEqual(schedule.schedule_id, changed.schedule_id)


if __name__ == "__main__":
    unittest.main()
