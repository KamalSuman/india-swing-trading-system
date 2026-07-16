from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum

from india_swing.identity import content_id


ZERO = Decimal("0")
ONE_HUNDRED = Decimal("100")
TEN_THOUSAND = Decimal("10000")
PAISE = Decimal("0.01")
RUPEE = Decimal("1")


class CostScheduleError(ValueError):
    pass


class FillSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class ZerodhaDpTariff(str, Enum):
    """Explicit tariff selection; account-holder attributes are never inferred."""

    RESIDENT_RETAIL_STANDARD = "RESIDENT_RETAIL_STANDARD"
    RESIDENT_RETAIL_FEMALE_FIRST_HOLDER = "RESIDENT_RETAIL_FEMALE_FIRST_HOLDER"


def _decimal(value: Decimal, name: str, *, positive: bool = False) -> None:
    if type(value) is not Decimal:
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    if positive and value <= ZERO:
        raise ValueError(f"{name} must be positive")
    if not positive and value < ZERO:
        raise ValueError(f"{name} cannot be negative")


def _paise(value: Decimal) -> Decimal:
    return value.quantize(PAISE, rounding=ROUND_HALF_UP)


def _rupee(value: Decimal) -> Decimal:
    return value.quantize(RUPEE, rounding=ROUND_HALF_UP)


def _bps(turnover: Decimal, rate: Decimal) -> Decimal:
    return turnover * rate / TEN_THOUSAND


@dataclass(frozen=True, slots=True)
class DeliveryFill:
    trade_date: date
    symbol: str
    isin: str
    side: FillSide
    quantity: int
    price: Decimal
    order_id: str
    fill_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.trade_date) is not date:
            raise TypeError("trade_date must be a date")
        for value, name in ((self.symbol, "symbol"), (self.isin, "isin")):
            if not isinstance(value, str) or not value or value != value.strip().upper():
                raise ValueError(f"{name} must be normalized uppercase text")
        if len(self.isin) != 12:
            raise ValueError("isin must contain 12 characters")
        if not isinstance(self.side, FillSide):
            raise TypeError("side must be a FillSide")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("quantity must be a positive integer")
        _decimal(self.price, "price", positive=True)
        if not isinstance(self.order_id, str) or not self.order_id.strip():
            raise ValueError("order_id is required")
        object.__setattr__(
            self,
            "fill_id",
            content_id(
                {
                    "schema": "nse-delivery-fill/v1",
                    "trade_date": self.trade_date,
                    "symbol": self.symbol,
                    "isin": self.isin,
                    "side": self.side,
                    "quantity": self.quantity,
                    "price": self.price,
                    "order_id": self.order_id,
                },
                length=64,
            ),
        )

    @property
    def turnover(self) -> Decimal:
        return self.price * self.quantity


@dataclass(frozen=True, slots=True)
class NseDeliveryCostSchedule:
    """One effective-dated Zerodha resident-retail NSE delivery tariff."""

    effective_from: date
    effective_to: date | None
    dp_tariff: ZerodhaDpTariff
    brokerage_bps: Decimal
    stt_buy_bps: Decimal
    stt_sell_bps: Decimal
    exchange_and_ipft_bps: Decimal
    sebi_bps: Decimal
    stamp_buy_bps: Decimal
    gst_rate: Decimal
    dp_base_per_scrip: Decimal
    source_urls: tuple[str, ...]
    policy_version: str = "zerodha-nse-equity-delivery-cost/v1"
    schedule_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.effective_from) is not date:
            raise TypeError("effective_from must be a date")
        if self.effective_to is not None:
            if type(self.effective_to) is not date:
                raise TypeError("effective_to must be a date or None")
            if self.effective_to < self.effective_from:
                raise ValueError("effective_to cannot precede effective_from")
        if not isinstance(self.dp_tariff, ZerodhaDpTariff):
            raise TypeError("dp_tariff must be a ZerodhaDpTariff")
        for name in (
            "brokerage_bps",
            "stt_buy_bps",
            "stt_sell_bps",
            "exchange_and_ipft_bps",
            "sebi_bps",
            "stamp_buy_bps",
            "gst_rate",
            "dp_base_per_scrip",
        ):
            _decimal(getattr(self, name), name)
        if self.gst_rate > Decimal("1"):
            raise ValueError("gst_rate must be expressed as a fraction")
        if (
            type(self.source_urls) is not tuple
            or not self.source_urls
            or any(not url.startswith("https://") for url in self.source_urls)
            or len(set(self.source_urls)) != len(self.source_urls)
        ):
            raise ValueError("source_urls must be unique HTTPS URLs")
        if not isinstance(self.policy_version, str) or not self.policy_version:
            raise ValueError("policy_version is required")
        object.__setattr__(
            self,
            "schedule_id",
            content_id(
                {
                    "schema": "nse-delivery-cost-schedule/v1",
                    "effective_from": self.effective_from,
                    "effective_to": self.effective_to,
                    "dp_tariff": self.dp_tariff,
                    "brokerage_bps": self.brokerage_bps,
                    "stt_buy_bps": self.stt_buy_bps,
                    "stt_sell_bps": self.stt_sell_bps,
                    "exchange_and_ipft_bps": self.exchange_and_ipft_bps,
                    "sebi_bps": self.sebi_bps,
                    "stamp_buy_bps": self.stamp_buy_bps,
                    "gst_rate": self.gst_rate,
                    "dp_base_per_scrip": self.dp_base_per_scrip,
                    "source_urls": self.source_urls,
                    "policy_version": self.policy_version,
                },
                length=64,
            ),
        )

    def applies_on(self, trade_date: date) -> bool:
        return self.effective_from <= trade_date and (
            self.effective_to is None or trade_date <= self.effective_to
        )


@dataclass(frozen=True, slots=True)
class DeliveryLegCharges:
    trade_date: date
    turnover: Decimal
    brokerage: Decimal
    stt: Decimal
    exchange_and_ipft: Decimal
    sebi: Decimal
    stamp: Decimal
    gst: Decimal
    dp_base: Decimal
    dp_gst: Decimal

    @property
    def dp_total(self) -> Decimal:
        return self.dp_base + self.dp_gst

    @property
    def total(self) -> Decimal:
        return (
            self.brokerage
            + self.stt
            + self.exchange_and_ipft
            + self.sebi
            + self.stamp
            + self.gst
            + self.dp_total
        )


@dataclass(frozen=True, slots=True)
class DeliveryChargeBreakdown:
    schedule_id: str
    legs: tuple[DeliveryLegCharges, ...]
    calculation_id: str = field(init=False)

    def __post_init__(self) -> None:
        if len(self.schedule_id) != 64:
            raise ValueError("schedule_id must be a full content ID")
        if type(self.legs) is not tuple or not self.legs:
            raise ValueError("at least one charge leg is required")
        if tuple(sorted(self.legs, key=lambda leg: leg.trade_date)) != self.legs:
            raise ValueError("charge legs must be ordered by trade date")
        if len({leg.trade_date for leg in self.legs}) != len(self.legs):
            raise ValueError("charge legs must have unique trade dates")
        object.__setattr__(
            self,
            "calculation_id",
            content_id(
                {
                    "schema": "nse-delivery-charge-breakdown/v1",
                    "schedule_id": self.schedule_id,
                    "legs": self.legs,
                },
                length=64,
            ),
        )

    @property
    def total(self) -> Decimal:
        return sum((leg.total for leg in self.legs), ZERO)


def zerodha_nse_delivery_schedule_2026(
    *,
    dp_tariff: ZerodhaDpTariff = ZerodhaDpTariff.RESIDENT_RETAIL_STANDARD,
) -> NseDeliveryCostSchedule:
    """Current rate card from 2026-03-01; not valid for earlier backtests."""

    dp_base = Decimal("13.00")
    if dp_tariff is ZerodhaDpTariff.RESIDENT_RETAIL_FEMALE_FIRST_HOLDER:
        dp_base = Decimal("12.75")
    return NseDeliveryCostSchedule(
        effective_from=date(2026, 3, 1),
        effective_to=None,
        dp_tariff=dp_tariff,
        brokerage_bps=Decimal("0"),
        stt_buy_bps=Decimal("10"),
        stt_sell_bps=Decimal("10"),
        exchange_and_ipft_bps=Decimal("0.307"),
        sebi_bps=Decimal("0.01"),
        stamp_buy_bps=Decimal("1.5"),
        gst_rate=Decimal("0.18"),
        dp_base_per_scrip=dp_base,
        source_urls=(
            "https://zerodha.com/charges",
            "https://nsearchives.nseindia.com/content/circulars/FA73061.pdf",
            "https://www.incometaxindia.gov.in/w/section-98-55",
            "https://support.zerodha.com/category/account-opening/resident-individual/ri-charges/articles/how-is-the-securities-transaction-tax-stt-calculated",
            "https://www.sebi.gov.in/sebi_data/attachdocs/aug-2021/1628678904669.pdf",
            "https://www.indiacode.nic.in/show-data?abv=CEN&actid=AC_CEN_2_2_00036_189902_1523339055436&orderno=18&orgactid=AC_CEN_2_2_00036_189902_1523339055436&sectionId=49724&sectionno=9A&statehandle=123456789%2F1362",
            "https://www.cdslindia.com/dp/dpdetails.aspx?dp_id=81600",
        ),
    )


def calculate_delivery_charges(
    fills: tuple[DeliveryFill, ...],
    schedule: NseDeliveryCostSchedule,
) -> DeliveryChargeBreakdown:
    """Calculate contract-day charges; STT and DP are aggregated, not per fill."""

    if type(fills) is not tuple or not fills:
        raise ValueError("fills must be a non-empty tuple")
    if type(schedule) is not NseDeliveryCostSchedule:
        raise TypeError("schedule must be an exact NseDeliveryCostSchedule")
    if len({fill.fill_id for fill in fills}) != len(fills):
        raise CostScheduleError("duplicate fills are not allowed")

    by_date: dict[date, list[DeliveryFill]] = defaultdict(list)
    for fill in fills:
        if type(fill) is not DeliveryFill:
            raise TypeError("every fill must be an exact DeliveryFill")
        if not schedule.applies_on(fill.trade_date):
            raise CostScheduleError(
                f"schedule {schedule.schedule_id} does not apply on {fill.trade_date}"
            )
        by_date[fill.trade_date].append(fill)

    legs: list[DeliveryLegCharges] = []
    for trade_date, day_fills in sorted(by_date.items()):
        sides_by_scrip: dict[tuple[str, str], set[FillSide]] = defaultdict(set)
        for fill in day_fills:
            sides_by_scrip[(fill.symbol, fill.isin)].add(fill.side)
        if any(len(sides) > 1 for sides in sides_by_scrip.values()):
            raise CostScheduleError(
                "same-day buys and sells of one scrip are ambiguous under a delivery tariff"
            )
        buy_turnover = sum(
            (fill.turnover for fill in day_fills if fill.side is FillSide.BUY), ZERO
        )
        sell_turnover = sum(
            (fill.turnover for fill in day_fills if fill.side is FillSide.SELL), ZERO
        )
        turnover = buy_turnover + sell_turnover

        brokerage = _paise(_bps(turnover, schedule.brokerage_bps))
        exchange_and_ipft = _paise(_bps(turnover, schedule.exchange_and_ipft_bps))
        sebi = _paise(_bps(turnover, schedule.sebi_bps))
        stamp = _rupee(_bps(buy_turnover, schedule.stamp_buy_bps))

        # Zerodha rounds STT to the nearest rupee at contract-note aggregation.
        stt_by_security = ZERO
        security_turnover: dict[tuple[str, str, FillSide], Decimal] = defaultdict(
            lambda: ZERO
        )
        for fill in day_fills:
            security_turnover[(fill.symbol, fill.isin, fill.side)] += fill.turnover
        for (_, _, side), value in security_turnover.items():
            rate = schedule.stt_buy_bps if side is FillSide.BUY else schedule.stt_sell_bps
            stt_by_security += _paise(_bps(value, rate))
        stt = _rupee(stt_by_security)

        gst = _paise((brokerage + exchange_and_ipft + sebi) * schedule.gst_rate)
        sold_scrips = {
            (fill.symbol, fill.isin)
            for fill in day_fills
            if fill.side is FillSide.SELL
        }
        dp_base = _paise(schedule.dp_base_per_scrip * len(sold_scrips))
        dp_gst = _paise(dp_base * schedule.gst_rate)
        legs.append(
            DeliveryLegCharges(
                trade_date=trade_date,
                turnover=_paise(turnover),
                brokerage=brokerage,
                stt=stt,
                exchange_and_ipft=exchange_and_ipft,
                sebi=sebi,
                stamp=stamp,
                gst=gst,
                dp_base=dp_base,
                dp_gst=dp_gst,
            )
        )

    return DeliveryChargeBreakdown(schedule_id=schedule.schedule_id, legs=tuple(legs))
