from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from enum import Enum

from india_swing.execution.costs import FillSide
from india_swing.identity import content_id


ZERO = Decimal("0")
TEN_THOUSAND = Decimal("10000")


class ExitReason(str, Enum):
    STOP = "STOP"
    TARGET = "TARGET"
    TIME = "TIME"


def _price(value: Decimal, name: str) -> None:
    if type(value) is not Decimal:
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value <= ZERO:
        raise ValueError(f"{name} must be finite and positive")


def _on_tick(value: Decimal, tick_size: Decimal) -> bool:
    return value % tick_size == ZERO


def _adverse_buy(price: Decimal, slippage_bps: Decimal, tick_size: Decimal) -> Decimal:
    raw = price * (Decimal("1") + slippage_bps / TEN_THOUSAND)
    return (raw / tick_size).to_integral_value(rounding=ROUND_CEILING) * tick_size


def _adverse_sell(price: Decimal, slippage_bps: Decimal, tick_size: Decimal) -> Decimal:
    raw = price * (Decimal("1") - slippage_bps / TEN_THOUSAND)
    return (raw / tick_size).to_integral_value(rounding=ROUND_FLOOR) * tick_size


@dataclass(frozen=True, slots=True)
class SimulationBar:
    session: date
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    tradable: bool = True
    lower_circuit_sell_locked: bool = False
    bar_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.session) is not date:
            raise TypeError("session must be a date")
        if not isinstance(self.symbol, str) or not self.symbol or self.symbol != self.symbol.strip().upper():
            raise ValueError("symbol must be normalized uppercase text")
        for name in ("open", "high", "low", "close"):
            _price(getattr(self, name), name)
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("high is inconsistent with OHLC prices")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("low is inconsistent with OHLC prices")
        if type(self.volume) is not int or self.volume < 0:
            raise ValueError("volume must be a non-negative integer")
        if self.tradable and self.volume == 0:
            raise ValueError("a tradable bar must have positive volume")
        if self.lower_circuit_sell_locked and not self.tradable:
            raise ValueError("a circuit-locked bar still needs an exchange trading session")
        object.__setattr__(
            self,
            "bar_id",
            content_id(
                {
                    "schema": "daily-execution-simulation-bar/v1",
                    "session": self.session,
                    "symbol": self.symbol,
                    "open": self.open,
                    "high": self.high,
                    "low": self.low,
                    "close": self.close,
                    "volume": self.volume,
                    "tradable": self.tradable,
                    "lower_circuit_sell_locked": self.lower_circuit_sell_locked,
                },
                length=64,
            ),
        )


@dataclass(frozen=True, slots=True)
class LimitEntryOrder:
    symbol: str
    signal_session: date
    first_eligible_session: date
    expiry_session: date
    quantity: int
    limit_price: Decimal
    tick_size: Decimal
    maximum_participation: Decimal = Decimal("0.0025")
    order_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or not self.symbol or self.symbol != self.symbol.strip().upper():
            raise ValueError("symbol must be normalized uppercase text")
        if any(type(value) is not date for value in (self.signal_session, self.first_eligible_session, self.expiry_session)):
            raise TypeError("order sessions must be dates")
        if self.first_eligible_session <= self.signal_session:
            raise ValueError("entry must start strictly after the signal session")
        if self.expiry_session < self.first_eligible_session:
            raise ValueError("expiry cannot precede the first eligible session")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("quantity must be positive")
        _price(self.limit_price, "limit_price")
        _price(self.tick_size, "tick_size")
        if not _on_tick(self.limit_price, self.tick_size):
            raise ValueError("limit_price must be an exact tick multiple")
        if type(self.maximum_participation) is not Decimal:
            raise TypeError("maximum_participation must be a Decimal")
        if not ZERO < self.maximum_participation <= Decimal("1"):
            raise ValueError("maximum_participation must be in (0, 1]")
        object.__setattr__(
            self,
            "order_id",
            content_id(
                {
                    "schema": "daily-limit-entry-order/v1",
                    "symbol": self.symbol,
                    "signal_session": self.signal_session,
                    "first_eligible_session": self.first_eligible_session,
                    "expiry_session": self.expiry_session,
                    "quantity": self.quantity,
                    "limit_price": self.limit_price,
                    "tick_size": self.tick_size,
                    "maximum_participation": self.maximum_participation,
                },
                length=64,
            ),
        )


@dataclass(frozen=True, slots=True)
class ProtectiveExitOrder:
    symbol: str
    quantity: int
    entry_session: date
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    tick_size: Decimal
    maximum_participation: Decimal = Decimal("0.0025")

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or not self.symbol or self.symbol != self.symbol.strip().upper():
            raise ValueError("symbol must be normalized uppercase text")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if type(self.entry_session) is not date:
            raise TypeError("entry_session must be a date")
        for name in ("entry_price", "stop_price", "target_price", "tick_size"):
            _price(getattr(self, name), name)
        if not self.stop_price < self.entry_price < self.target_price:
            raise ValueError("protective prices must satisfy stop < entry < target")
        if any(
            not _on_tick(value, self.tick_size)
            for value in (self.entry_price, self.stop_price, self.target_price)
        ):
            raise ValueError("entry, stop, and target must be exact tick multiples")
        if type(self.maximum_participation) is not Decimal:
            raise TypeError("maximum_participation must be a Decimal")
        if not ZERO < self.maximum_participation <= Decimal("1"):
            raise ValueError("maximum_participation must be in (0, 1]")


@dataclass(frozen=True, slots=True)
class SimulatedFill:
    order_id: str
    bar_id: str
    session: date
    symbol: str
    side: FillSide
    quantity: int
    trigger_price: Decimal
    fill_price: Decimal
    slippage_bps: Decimal
    exit_reason: ExitReason | None
    fill_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in ((self.order_id, "order_id"), (self.bar_id, "bar_id")):
            if not isinstance(value, str) or len(value) != 64:
                raise ValueError(f"{name} must be a full content ID")
        _price(self.trigger_price, "trigger_price")
        _price(self.fill_price, "fill_price")
        if type(self.slippage_bps) is not Decimal or self.slippage_bps < ZERO:
            raise ValueError("slippage_bps must be a non-negative Decimal")
        object.__setattr__(
            self,
            "fill_id",
            content_id(
                {
                    "schema": "daily-simulated-fill/v1",
                    "order_id": self.order_id,
                    "bar_id": self.bar_id,
                    "session": self.session,
                    "symbol": self.symbol,
                    "side": self.side,
                    "quantity": self.quantity,
                    "trigger_price": self.trigger_price,
                    "fill_price": self.fill_price,
                    "slippage_bps": self.slippage_bps,
                    "exit_reason": self.exit_reason,
                },
                length=64,
            ),
        )


def _validate_slippage(slippage_bps: Decimal) -> None:
    if type(slippage_bps) is not Decimal or not slippage_bps.is_finite() or slippage_bps < ZERO:
        raise ValueError("slippage_bps must be a finite non-negative Decimal")


def simulate_limit_entry(
    order: LimitEntryOrder,
    bar: SimulationBar,
    *,
    slippage_bps: Decimal,
) -> SimulatedFill | None:
    """Fill a next-session buy limit without using the signal-session bar."""

    _validate_slippage(slippage_bps)
    if order.symbol != bar.symbol:
        raise ValueError("order and bar symbols differ")
    if not order.first_eligible_session <= bar.session <= order.expiry_session:
        return None
    if not bar.tradable or order.quantity > int(Decimal(bar.volume) * order.maximum_participation):
        return None
    if bar.open <= order.limit_price:
        trigger = bar.open
    elif bar.low <= order.limit_price:
        trigger = order.limit_price
    else:
        return None
    fill_price = min(
        order.limit_price,
        _adverse_buy(trigger, slippage_bps, order.tick_size),
    )
    return SimulatedFill(
        order_id=order.order_id,
        bar_id=bar.bar_id,
        session=bar.session,
        symbol=bar.symbol,
        side=FillSide.BUY,
        quantity=order.quantity,
        trigger_price=trigger,
        fill_price=fill_price,
        slippage_bps=slippage_bps,
        exit_reason=None,
    )


def simulate_protective_exit(
    order: ProtectiveExitOrder,
    bar: SimulationBar,
    *,
    slippage_bps: Decimal,
) -> SimulatedFill | None:
    """Resolve stop before target when daily OHLC cannot reveal intraday order."""

    _validate_slippage(slippage_bps)
    if order.symbol != bar.symbol:
        raise ValueError("order and bar symbols differ")
    if bar.session < order.entry_session:
        return None
    if (
        not bar.tradable
        or order.quantity > int(Decimal(bar.volume) * order.maximum_participation)
    ):
        return None

    stop_touched = bar.low <= order.stop_price
    target_touched = bar.high >= order.target_price
    same_as_entry = bar.session == order.entry_session
    if stop_touched:
        if bar.lower_circuit_sell_locked:
            return None
        trigger = (
            order.stop_price
            if same_as_entry
            else bar.open if bar.open <= order.stop_price else order.stop_price
        )
        fill_price = _adverse_sell(trigger, slippage_bps, order.tick_size)
        reason = ExitReason.STOP
    elif target_touched:
        if same_as_entry:
            # Daily OHLC cannot prove the target happened after the entry. Carry
            # the position rather than booking an optimistic same-bar profit.
            return None
        trigger = bar.open if bar.open >= order.target_price else order.target_price
        fill_price = max(
            order.target_price,
            _adverse_sell(trigger, slippage_bps, order.tick_size),
        )
        reason = ExitReason.TARGET
    else:
        return None

    order_id = content_id(
        {
            "schema": "daily-protective-exit-order/v1",
            "symbol": order.symbol,
            "quantity": order.quantity,
            "entry_session": order.entry_session,
            "entry_price": order.entry_price,
            "stop_price": order.stop_price,
            "target_price": order.target_price,
            "tick_size": order.tick_size,
            "maximum_participation": order.maximum_participation,
        },
        length=64,
    )
    return SimulatedFill(
        order_id=order_id,
        bar_id=bar.bar_id,
        session=bar.session,
        symbol=bar.symbol,
        side=FillSide.SELL,
        quantity=order.quantity,
        trigger_price=trigger,
        fill_price=fill_price,
        slippage_bps=slippage_bps,
        exit_reason=reason,
    )


def simulate_time_exit(
    order: ProtectiveExitOrder,
    bar: SimulationBar,
    *,
    slippage_bps: Decimal,
) -> SimulatedFill | None:
    """Conservatively liquidate at a tradable closing price after the horizon."""

    _validate_slippage(slippage_bps)
    if order.symbol != bar.symbol:
        raise ValueError("order and bar symbols differ")
    if bar.session < order.entry_session:
        return None
    if (
        not bar.tradable
        or bar.lower_circuit_sell_locked
        or order.quantity > int(Decimal(bar.volume) * order.maximum_participation)
    ):
        return None
    order_id = content_id(
        {
            "schema": "daily-time-exit-order/v1",
            "symbol": order.symbol,
            "quantity": order.quantity,
            "entry_session": order.entry_session,
            "entry_price": order.entry_price,
            "stop_price": order.stop_price,
            "target_price": order.target_price,
            "tick_size": order.tick_size,
            "maximum_participation": order.maximum_participation,
            "exit_session": bar.session,
        },
        length=64,
    )
    return SimulatedFill(
        order_id=order_id,
        bar_id=bar.bar_id,
        session=bar.session,
        symbol=bar.symbol,
        side=FillSide.SELL,
        quantity=order.quantity,
        trigger_price=bar.close,
        fill_price=_adverse_sell(bar.close, slippage_bps, order.tick_size),
        slippage_bps=slippage_bps,
        exit_reason=ExitReason.TIME,
    )
