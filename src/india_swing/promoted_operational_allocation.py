"""Pure, deterministic capital allocation over promoted operational quote
PASS outcomes.

Consumes one already-verified ``VerifiedPromotedOperationalQuoteGateBatch``
plus an exact, evidence-bound aggregate portfolio context and allocates
only quote-gate PASS candidates under current cash, risk, exposure,
position, research-liquidity, and top-of-book constraints. Performs no I/O,
environment, clock, network, broker, filesystem, GCP, notification, or
persistence access. Operational quantity may only stay equal to or decrease
from the retained research-intent quantity; no price, stop, target, tick,
cost buffer, or holding period is ever changed. Produces no BUY decision,
notification, or execution authority: every batch and outcome here is
``paper_only=True`` and permanently ``notification_eligible=False``/
``execution_eligible=False``.
"""

from __future__ import annotations

import decimal
import re
from dataclasses import dataclass, field, fields
from decimal import ROUND_FLOOR, Decimal
from enum import Enum
from typing import NamedTuple

from india_swing.identity import content_id
from india_swing.promoted_operational_quote_gate import (
    PromotedOperationalQuoteOutcome,
    VerifiedPromotedOperationalQuoteGateBatch,
)
from india_swing.risk.swing_portfolio import (
    SwingPortfolioSizingError,
    SwingPortfolioSizingPolicy,
    SwingPortfolioSnapshot,
)
from india_swing.signals.quote_gate import SwingQuoteGateDisposition


ZERO = Decimal("0")

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_LISTING_KEY = re.compile(r"NSE:[A-Z0-9&.\-]{1,32}\Z")

PROMOTED_OPERATIONAL_PORTFOLIO_CONTEXT_SCHEMA_VERSION = (
    "promoted-operational-portfolio-context/v1"
)
PROMOTED_OPERATIONAL_ALLOCATION_POLICY_SCHEMA_VERSION = (
    "promoted-operational-allocation-policy/v1"
)
PROMOTED_OPERATIONAL_ALLOCATION_STATE_SCHEMA_VERSION = (
    "promoted-operational-allocation-state/v1"
)
PROMOTED_OPERATIONAL_ALLOCATION_OUTCOME_SCHEMA_VERSION = (
    "promoted-operational-allocation-outcome/v1"
)
PROMOTED_OPERATIONAL_ALLOCATION_BATCH_SCHEMA_VERSION = (
    "promoted-operational-allocation-batch/v1"
)
PROMOTED_OPERATIONAL_ALLOCATION_EVIDENCE_SCHEMA_VERSION = (
    "promoted-operational-allocation-evidence/v1"
)


class PromotedOperationalAllocationError(ValueError):
    pass


_ERR_TYPE = "promoted operational allocation type is invalid"
_ERR_CONTEXT = "promoted operational portfolio context is invalid"
_ERR_POLICY = "promoted operational allocation policy is invalid"
_ERR_AUTHORITY = "promoted operational allocation authority flags are invalid"
_ERR_STATE = "promoted operational allocation state is invalid"
_ERR_QUOTE = "promoted operational allocation quote input is invalid"
_ERR_OUTCOME = "promoted operational allocation outcome is invalid"
_ERR_COVERAGE = "promoted operational allocation coverage is invalid"
_ERR_PORTFOLIO = "promoted operational allocation portfolio snapshot is invalid"
_ERR_REPLAY = "promoted operational allocation could not replay"
_ERR_EVIDENCE = "promoted operational allocation evidence is invalid"


class PromotedOperationalAllocationDisposition(str, Enum):
    ALLOCATED = "ALLOCATED"
    VETO = "VETO"


class PromotedOperationalAllocationReason(str, Enum):
    DUPLICATE_OPEN_LISTING = "DUPLICATE_OPEN_LISTING"
    DAILY_LOSS_HALT = "DAILY_LOSS_HALT"
    PILOT_DRAWDOWN_HALT = "PILOT_DRAWDOWN_HALT"
    MAX_OPEN_POSITIONS_REACHED = "MAX_OPEN_POSITIONS_REACHED"
    MAX_NEW_POSITIONS_PER_RUN_REACHED = "MAX_NEW_POSITIONS_PER_RUN_REACHED"
    RESEARCH_LIQUIDITY_POLICY_TOO_WIDE = "RESEARCH_LIQUIDITY_POLICY_TOO_WIDE"
    PER_TRADE_RISK_TOO_SMALL = "PER_TRADE_RISK_TOO_SMALL"
    TOTAL_OPEN_RISK_EXHAUSTED = "TOTAL_OPEN_RISK_EXHAUSTED"
    POSITION_NOTIONAL_CAP_TOO_SMALL = "POSITION_NOTIONAL_CAP_TOO_SMALL"
    GROSS_EXPOSURE_EXHAUSTED = "GROSS_EXPOSURE_EXHAUSTED"
    CASH_EXHAUSTED = "CASH_EXHAUSTED"
    ASK_DEPTH_CAP_TOO_SMALL = "ASK_DEPTH_CAP_TOO_SMALL"
    NET_REWARD_RISK_BELOW_MINIMUM = "NET_REWARD_RISK_BELOW_MINIMUM"


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _canonical_listing_keys(value: object) -> bool:
    return (
        type(value) is tuple
        and not any(
            type(item) is not str or _LISTING_KEY.fullmatch(item) is None
            for item in value
        )
        and len(set(value)) == len(value)
        and tuple(sorted(value)) == value
    )


def _replay_portfolio_snapshot(portfolio: SwingPortfolioSnapshot) -> None:
    """Independently reconstruct ``portfolio`` from its own retained fields
    so its ``__post_init__`` semantic ceilings run again -- ``portfolio``'s
    own ``verify_content_identity`` only compares hashes, which a
    self-consistently rehashed, semantically invalid snapshot would pass
    trivially."""

    if type(portfolio) is not SwingPortfolioSnapshot:
        raise PromotedOperationalAllocationError(_ERR_PORTFOLIO)
    identity = {
        item.name: getattr(portfolio, item.name)
        for item in fields(portfolio)
        if item.name != "portfolio_snapshot_id"
    }
    try:
        fresh = SwingPortfolioSnapshot(**identity)
    except SwingPortfolioSizingError:
        raise PromotedOperationalAllocationError(_ERR_PORTFOLIO) from None
    except Exception:
        raise PromotedOperationalAllocationError(_ERR_PORTFOLIO) from None
    if fresh.portfolio_snapshot_id != portfolio.portfolio_snapshot_id:
        raise PromotedOperationalAllocationError(_ERR_PORTFOLIO)


def _replay_sizing_policy(policy: SwingPortfolioSizingPolicy) -> None:
    """Independently reconstruct ``policy`` from its own retained fields so
    its ``__post_init__`` semantic ceilings run again -- ``policy``'s own
    ``verify_content_identity`` only compares hashes, which a
    self-consistently rehashed, widened policy (for example
    ``per_trade_risk_fraction=2``) would pass trivially."""

    if type(policy) is not SwingPortfolioSizingPolicy:
        raise PromotedOperationalAllocationError(_ERR_POLICY)
    identity = {
        item.name: getattr(policy, item.name)
        for item in fields(policy)
        if item.name != "policy_id"
    }
    try:
        fresh = SwingPortfolioSizingPolicy(**identity)
    except SwingPortfolioSizingError:
        raise PromotedOperationalAllocationError(_ERR_POLICY) from None
    except Exception:
        raise PromotedOperationalAllocationError(_ERR_POLICY) from None
    if fresh.policy_id != policy.policy_id:
        raise PromotedOperationalAllocationError(_ERR_POLICY)


@dataclass(frozen=True, slots=True)
class PromotedOperationalPortfolioContext:
    """Binds one exact ``SwingPortfolioSnapshot`` to the externally supplied
    source artifact it came from and the exact sorted unique open-listing
    key set it holds -- the snapshot itself does not identify open symbols,
    so duplicate-symbol protection is never guessed."""

    portfolio: SwingPortfolioSnapshot
    source_portfolio_artifact_id: str
    open_listing_keys: tuple[str, ...]
    context_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.portfolio) is not SwingPortfolioSnapshot:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        _replay_portfolio_snapshot(self.portfolio)
        if not _sha(self.source_portfolio_artifact_id):
            raise PromotedOperationalAllocationError(_ERR_CONTEXT)
        if not _canonical_listing_keys(self.open_listing_keys):
            raise PromotedOperationalAllocationError(_ERR_CONTEXT)
        if len(self.open_listing_keys) != self.portfolio.open_positions:
            raise PromotedOperationalAllocationError(_ERR_CONTEXT)
        object.__setattr__(self, "context_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "context_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_PORTFOLIO_CONTEXT_SCHEMA_VERSION,
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedOperationalPortfolioContext:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        _replay_portfolio_snapshot(self.portfolio)
        try:
            fresh = PromotedOperationalPortfolioContext(**self._identity())
        except PromotedOperationalAllocationError:
            raise
        except Exception:
            raise PromotedOperationalAllocationError(_ERR_TYPE) from None
        if self.context_id != fresh.context_id:
            raise PromotedOperationalAllocationError(_ERR_TYPE)


@dataclass(frozen=True, slots=True)
class PromotedOperationalAllocationPolicy:
    """Binds one exact ``SwingPortfolioSizingPolicy`` plus the maximum
    portfolio-snapshot age this allocation accepts. Grants no authority:
    ``paper_only`` is always true and both
    ``notification_eligible``/``execution_eligible`` are always false."""

    policy: SwingPortfolioSizingPolicy
    maximum_portfolio_age_seconds: int = 300
    paper_only: bool = True
    notification_eligible: bool = False
    execution_eligible: bool = False
    allocation_policy_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.policy) is not SwingPortfolioSizingPolicy:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        _replay_sizing_policy(self.policy)
        if (
            type(self.maximum_portfolio_age_seconds) is not int
            or self.maximum_portfolio_age_seconds <= 0
        ):
            raise PromotedOperationalAllocationError(_ERR_POLICY)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalAllocationError(_ERR_AUTHORITY)
        object.__setattr__(self, "allocation_policy_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "allocation_policy_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_ALLOCATION_POLICY_SCHEMA_VERSION,
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedOperationalAllocationPolicy:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        _replay_sizing_policy(self.policy)
        try:
            fresh = PromotedOperationalAllocationPolicy(**self._identity())
        except PromotedOperationalAllocationError:
            raise
        except Exception:
            raise PromotedOperationalAllocationError(_ERR_TYPE) from None
        if self.allocation_policy_id != fresh.allocation_policy_id:
            raise PromotedOperationalAllocationError(_ERR_TYPE)


@dataclass(frozen=True, slots=True)
class PromotedOperationalAllocationState:
    """Running allocation state within one batch. ``open_position_count`` is
    always ``len(open_listing_keys)`` -- never an independent field that
    could drift out of sync."""

    cash_available: Decimal
    gross_exposure: Decimal
    open_risk: Decimal
    open_listing_keys: tuple[str, ...]
    state_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value in (self.cash_available, self.gross_exposure, self.open_risk):
            if type(value) is not Decimal or not value.is_finite() or value < ZERO:
                raise PromotedOperationalAllocationError(_ERR_STATE)
        if not _canonical_listing_keys(self.open_listing_keys):
            raise PromotedOperationalAllocationError(_ERR_STATE)
        object.__setattr__(self, "state_id", self._calculated_id())

    @property
    def open_position_count(self) -> int:
        return len(self.open_listing_keys)

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "state_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_ALLOCATION_STATE_SCHEMA_VERSION,
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedOperationalAllocationState:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        try:
            fresh = PromotedOperationalAllocationState(**self._identity())
        except PromotedOperationalAllocationError:
            raise
        except Exception:
            raise PromotedOperationalAllocationError(_ERR_TYPE) from None
        if self.state_id != fresh.state_id:
            raise PromotedOperationalAllocationError(_ERR_TYPE)


def _top_ask_quantity(quote_outcome: PromotedOperationalQuoteOutcome) -> int:
    best_ask = quote_outcome.quote.best_ask
    if best_ask is None:
        raise PromotedOperationalAllocationError(_ERR_QUOTE)
    for level in quote_outcome.quote.depth_sell:
        if level.price == best_ask:
            return level.quantity
    raise PromotedOperationalAllocationError(_ERR_QUOTE)


def _floor_units(value: Decimal) -> int:
    if value <= ZERO:
        return 0
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


class _AllocationCeilings(NamedTuple):
    """Every independently-derived allocation quantity ceiling plus the
    feasible quantity before any non-quantity veto is applied. Shared
    unchanged between ``_evaluate_allocation`` and the allocation-evidence
    builder so evidence can never diverge from the actual calculation.

    ``feasible_quantity`` is mathematically identical to allocation's
    original single combined ``min(per_trade_budget, remaining_open_risk) /
    loss_per_share`` risk cap: for any positive ``c``,
    ``floor(min(a, b) / c) == min(floor(a / c), floor(b / c))``, so
    splitting that one combined ceiling into the two separate
    ``per_trade_risk_quantity``/``total_open_risk_quantity`` ceilings named
    below and taking their min alongside every other ceiling reproduces the
    exact same feasible quantity as before.
    """

    research_quantity: int
    per_trade_risk_quantity: int
    total_open_risk_quantity: int
    position_notional_quantity: int
    gross_exposure_quantity: int
    cash_quantity: int
    ask_depth_quantity: int
    feasible_quantity: int
    loss_per_share: Decimal
    reward_per_share: Decimal
    net_reward_risk: Decimal


def _compute_allocation_ceilings(
    quote_outcome: PromotedOperationalQuoteOutcome,
    portfolio_context: PromotedOperationalPortfolioContext,
    allocation_policy: PromotedOperationalAllocationPolicy,
    state_before: PromotedOperationalAllocationState,
) -> _AllocationCeilings:
    """Pure, replay-verifiable computation of every allocation quantity
    ceiling and the feasible quantity before any non-quantity veto. All
    Decimal arithmetic runs inside an explicit local context so the result
    never depends on the caller's ambient global Decimal precision/
    rounding.
    """

    candidate = quote_outcome.candidate
    entry_order = candidate.research_intent.evaluation_intent.entry_order
    stop_price = candidate.research_intent.evaluation_intent.stop_price
    target_price = candidate.research_intent.evaluation_intent.target_price
    cost_buffer = candidate.research_intent.estimated_cost_buffer
    reference_entry_price = quote_outcome.reference_entry_price
    if reference_entry_price is None:
        raise PromotedOperationalAllocationError(_ERR_QUOTE)

    with decimal.localcontext() as ctx:
        ctx.prec = 50
        ctx.rounding = decimal.ROUND_HALF_EVEN

        loss_per_share = reference_entry_price - stop_price + cost_buffer
        reward_per_share = target_price - reference_entry_price - cost_buffer
        if loss_per_share <= ZERO or reward_per_share <= ZERO:
            raise PromotedOperationalAllocationError(_ERR_QUOTE)
        net_reward_risk = reward_per_share / loss_per_share

        policy = allocation_policy.policy
        portfolio = portfolio_context.portfolio
        capital = portfolio.capital
        per_trade_budget = capital * policy.per_trade_risk_fraction
        maximum_open_risk = capital * policy.maximum_total_open_risk_fraction
        maximum_position_notional = capital * policy.maximum_position_notional_fraction
        maximum_gross_exposure = capital * policy.maximum_gross_exposure_fraction
        remaining_open_risk = max(ZERO, maximum_open_risk - state_before.open_risk)
        remaining_gross_exposure = max(
            ZERO, maximum_gross_exposure - state_before.gross_exposure
        )

        research_quantity = entry_order.quantity
        per_trade_risk_quantity = _floor_units(per_trade_budget / loss_per_share)
        total_open_risk_quantity = _floor_units(remaining_open_risk / loss_per_share)
        position_notional_quantity = _floor_units(
            maximum_position_notional / reference_entry_price
        )
        gross_exposure_quantity = _floor_units(
            remaining_gross_exposure / reference_entry_price
        )
        cash_quantity = _floor_units(
            state_before.cash_available / (reference_entry_price + cost_buffer)
        )
        ask_depth_quantity = _floor_units(
            Decimal(_top_ask_quantity(quote_outcome)) * policy.maximum_top_ask_participation
        )
        feasible_quantity = min(
            research_quantity,
            per_trade_risk_quantity,
            total_open_risk_quantity,
            position_notional_quantity,
            gross_exposure_quantity,
            cash_quantity,
            ask_depth_quantity,
        )

        return _AllocationCeilings(
            research_quantity=research_quantity,
            per_trade_risk_quantity=per_trade_risk_quantity,
            total_open_risk_quantity=total_open_risk_quantity,
            position_notional_quantity=position_notional_quantity,
            gross_exposure_quantity=gross_exposure_quantity,
            cash_quantity=cash_quantity,
            ask_depth_quantity=ask_depth_quantity,
            feasible_quantity=feasible_quantity,
            loss_per_share=loss_per_share,
            reward_per_share=reward_per_share,
            net_reward_risk=net_reward_risk,
        )


_CEILING_CODE_RESEARCH_QUANTITY = "RESEARCH_QUANTITY"
_CEILING_CODE_PER_TRADE_RISK = "PER_TRADE_RISK"
_CEILING_CODE_TOTAL_OPEN_RISK = "TOTAL_OPEN_RISK"
_CEILING_CODE_POSITION_NOTIONAL = "POSITION_NOTIONAL"
_CEILING_CODE_GROSS_EXPOSURE = "GROSS_EXPOSURE"
_CEILING_CODE_CASH = "CASH"
_CEILING_CODE_ASK_DEPTH_PARTICIPATION = "ASK_DEPTH_PARTICIPATION"


def _binding_ceiling_codes(ceilings: _AllocationCeilings) -> tuple[str, ...]:
    """Canonical, sorted, unique codes for every ceiling exactly equal to
    the feasible quantity -- the ceiling(s) that actually bound it. More
    than one code can tie."""

    pairs = (
        (_CEILING_CODE_RESEARCH_QUANTITY, ceilings.research_quantity),
        (_CEILING_CODE_PER_TRADE_RISK, ceilings.per_trade_risk_quantity),
        (_CEILING_CODE_TOTAL_OPEN_RISK, ceilings.total_open_risk_quantity),
        (_CEILING_CODE_POSITION_NOTIONAL, ceilings.position_notional_quantity),
        (_CEILING_CODE_GROSS_EXPOSURE, ceilings.gross_exposure_quantity),
        (_CEILING_CODE_CASH, ceilings.cash_quantity),
        (_CEILING_CODE_ASK_DEPTH_PARTICIPATION, ceilings.ask_depth_quantity),
    )
    return tuple(
        sorted(code for code, value in pairs if value == ceilings.feasible_quantity)
    )


def _evaluate_allocation(
    quote_outcome: PromotedOperationalQuoteOutcome,
    portfolio_context: PromotedOperationalPortfolioContext,
    allocation_policy: PromotedOperationalAllocationPolicy,
    state_before: PromotedOperationalAllocationState,
) -> tuple[
    PromotedOperationalAllocationDisposition,
    tuple[str, ...],
    int,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    PromotedOperationalAllocationState,
]:
    """Pure, replay-verifiable allocation of one exact PASS candidate.

    Never changes quantity, limit, stop, target, tick, cost buffer, or
    holding period retained inside the research intent. Shares its quantity
    ceilings with ``_compute_allocation_ceilings`` (also used by the
    allocation-evidence builder) so evidence can never diverge from the
    actual allocation calculation. All Decimal arithmetic runs inside an
    explicit local context so the result never depends on the caller's
    ambient global Decimal precision/rounding.
    """

    candidate = quote_outcome.candidate
    entry_order = candidate.research_intent.evaluation_intent.entry_order
    ceilings = _compute_allocation_ceilings(
        quote_outcome, portfolio_context, allocation_policy, state_before
    )

    with decimal.localcontext() as ctx:
        ctx.prec = 50
        ctx.rounding = decimal.ROUND_HALF_EVEN

        policy = allocation_policy.policy
        portfolio = portfolio_context.portfolio
        capital = portfolio.capital
        daily_loss_limit = capital * policy.maximum_daily_loss_fraction
        pilot_drawdown_limit = capital * policy.maximum_pilot_drawdown_fraction

        reasons: set[str] = set()
        if candidate.listing_key in state_before.open_listing_keys:
            reasons.add(PromotedOperationalAllocationReason.DUPLICATE_OPEN_LISTING.value)
        if entry_order.maximum_participation > policy.maximum_daily_turnover_participation:
            reasons.add(
                PromotedOperationalAllocationReason.RESEARCH_LIQUIDITY_POLICY_TOO_WIDE.value
            )
        if portfolio.daily_realized_pnl <= -daily_loss_limit:
            reasons.add(PromotedOperationalAllocationReason.DAILY_LOSS_HALT.value)
        if portfolio.pilot_realized_pnl <= -pilot_drawdown_limit:
            reasons.add(PromotedOperationalAllocationReason.PILOT_DRAWDOWN_HALT.value)
        if state_before.open_position_count >= policy.maximum_open_positions:
            reasons.add(PromotedOperationalAllocationReason.MAX_OPEN_POSITIONS_REACHED.value)
        if (
            state_before.open_position_count - portfolio.open_positions
            >= policy.maximum_new_positions_per_run
        ):
            reasons.add(
                PromotedOperationalAllocationReason.MAX_NEW_POSITIONS_PER_RUN_REACHED.value
            )
        if ceilings.net_reward_risk < policy.minimum_net_reward_risk:
            reasons.add(
                PromotedOperationalAllocationReason.NET_REWARD_RISK_BELOW_MINIMUM.value
            )
        if ceilings.per_trade_risk_quantity < 1:
            reasons.add(PromotedOperationalAllocationReason.PER_TRADE_RISK_TOO_SMALL.value)
        if ceilings.total_open_risk_quantity < 1:
            reasons.add(PromotedOperationalAllocationReason.TOTAL_OPEN_RISK_EXHAUSTED.value)
        if ceilings.position_notional_quantity < 1:
            reasons.add(
                PromotedOperationalAllocationReason.POSITION_NOTIONAL_CAP_TOO_SMALL.value
            )
        if ceilings.gross_exposure_quantity < 1:
            reasons.add(PromotedOperationalAllocationReason.GROSS_EXPOSURE_EXHAUSTED.value)
        if ceilings.cash_quantity < 1:
            reasons.add(PromotedOperationalAllocationReason.CASH_EXHAUSTED.value)
        if ceilings.ask_depth_quantity < 1:
            reasons.add(PromotedOperationalAllocationReason.ASK_DEPTH_CAP_TOO_SMALL.value)

        quantity = ceilings.feasible_quantity
        if reasons or quantity < 1:
            if quantity < 1 and not reasons:
                raise PromotedOperationalAllocationError(_ERR_QUOTE)
            return (
                PromotedOperationalAllocationDisposition.VETO,
                tuple(sorted(reasons)),
                0,
                ZERO,
                ZERO,
                ZERO,
                ceilings.net_reward_risk,
                state_before,
            )

        reference_entry_price = quote_outcome.reference_entry_price
        cost_buffer = candidate.research_intent.estimated_cost_buffer
        entry_notional = reference_entry_price * quantity
        estimated_round_trip_cost = cost_buffer * quantity
        planned_max_loss = ceilings.loss_per_share * quantity
        state_after = PromotedOperationalAllocationState(
            cash_available=(
                state_before.cash_available - entry_notional - estimated_round_trip_cost
            ),
            gross_exposure=state_before.gross_exposure + entry_notional,
            open_risk=state_before.open_risk + planned_max_loss,
            open_listing_keys=tuple(
                sorted(state_before.open_listing_keys + (candidate.listing_key,))
            ),
        )
        return (
            PromotedOperationalAllocationDisposition.ALLOCATED,
            (),
            quantity,
            entry_notional,
            estimated_round_trip_cost,
            planned_max_loss,
            ceilings.net_reward_risk,
            state_after,
        )


@dataclass(frozen=True, slots=True)
class PromotedOperationalAllocationOutcome:
    """One deterministic, replayable ALLOCATED/VETO outcome for one exact
    promoted operational quote PASS outcome.

    Never changes quantity, limit, stop, target, tick, cost buffer,
    reward/risk, or holding period retained inside the research intent.
    ``operational_quantity`` never exceeds the retained research quantity.
    """

    quote_outcome: PromotedOperationalQuoteOutcome
    portfolio_context: PromotedOperationalPortfolioContext
    allocation_policy: PromotedOperationalAllocationPolicy
    state_before: PromotedOperationalAllocationState
    state_after: PromotedOperationalAllocationState
    disposition: PromotedOperationalAllocationDisposition
    reason_codes: tuple[str, ...]
    operational_quantity: int
    reference_entry_price: Decimal
    entry_notional: Decimal
    estimated_round_trip_cost: Decimal
    planned_max_loss: Decimal
    operational_net_reward_risk: Decimal
    allocation_outcome_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._verify()
        object.__setattr__(self, "allocation_outcome_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.quote_outcome) is not PromotedOperationalQuoteOutcome:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.quote_outcome.verify_content_identity()
        if self.quote_outcome.disposition is not SwingQuoteGateDisposition.PASS:
            raise PromotedOperationalAllocationError(_ERR_QUOTE)
        if type(self.portfolio_context) is not PromotedOperationalPortfolioContext:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.portfolio_context.verify_content_identity()
        if type(self.allocation_policy) is not PromotedOperationalAllocationPolicy:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.allocation_policy.verify_content_identity()
        if (
            type(self.state_before) is not PromotedOperationalAllocationState
            or type(self.state_after) is not PromotedOperationalAllocationState
        ):
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.state_before.verify_content_identity()
        self.state_after.verify_content_identity()
        if type(self.disposition) is not PromotedOperationalAllocationDisposition:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        if (
            type(self.reference_entry_price) is not Decimal
            or not self.reference_entry_price.is_finite()
            or self.reference_entry_price <= ZERO
        ):
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        for value in (
            self.entry_notional,
            self.estimated_round_trip_cost,
            self.planned_max_loss,
        ):
            if type(value) is not Decimal or not value.is_finite() or value < ZERO:
                raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        if (
            type(self.operational_net_reward_risk) is not Decimal
            or not self.operational_net_reward_risk.is_finite()
        ):
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)

        try:
            replayed = _evaluate_allocation(
                self.quote_outcome,
                self.portfolio_context,
                self.allocation_policy,
                self.state_before,
            )
        except PromotedOperationalAllocationError:
            raise
        except Exception:
            raise PromotedOperationalAllocationError(_ERR_REPLAY) from None
        (
            disposition,
            reasons,
            quantity,
            notional,
            cost,
            loss,
            net_reward_risk,
            state_after,
        ) = replayed

        if disposition is not self.disposition:
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        if type(self.reason_codes) is not tuple or self.reason_codes != reasons:
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        if type(self.operational_quantity) is not int or self.operational_quantity != quantity:
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        if self.reference_entry_price != self.quote_outcome.reference_entry_price:
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        for actual, expected in (
            (self.entry_notional, notional),
            (self.estimated_round_trip_cost, cost),
            (self.planned_max_loss, loss),
            (self.operational_net_reward_risk, net_reward_risk),
        ):
            if actual != expected:
                raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        if self.state_after.state_id != state_after.state_id:
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)

        research_quantity = (
            self.quote_outcome.candidate.research_intent.evaluation_intent.entry_order.quantity
        )
        if self.operational_quantity > research_quantity:
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)

        if self.disposition is PromotedOperationalAllocationDisposition.ALLOCATED:
            if self.reason_codes or self.operational_quantity <= 0:
                raise PromotedOperationalAllocationError(_ERR_OUTCOME)
        else:
            if (
                not self.reason_codes
                or self.operational_quantity != 0
                or self.entry_notional != ZERO
                or self.estimated_round_trip_cost != ZERO
                or self.planned_max_loss != ZERO
                or self.state_after.state_id != self.state_before.state_id
            ):
                raise PromotedOperationalAllocationError(_ERR_OUTCOME)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_ALLOCATION_OUTCOME_SCHEMA_VERSION,
                **{
                    value.name: getattr(self, value.name)
                    for value in fields(self)
                    if value.name != "allocation_outcome_id"
                },
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.allocation_outcome_id != self._calculated_id():
            raise PromotedOperationalAllocationError(_ERR_OUTCOME)

    @property
    def allocated(self) -> bool:
        return self.disposition is PromotedOperationalAllocationDisposition.ALLOCATED

    @property
    def execution_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class PromotedOperationalAllocationEvidence:
    """Replayable quantity-ceiling and reward/risk evidence for one exact
    ``PromotedOperationalAllocationOutcome``, derived through the identical
    calculation path ``_evaluate_allocation`` itself uses
    (``_compute_allocation_ceilings``) rather than a divergent copy.
    Produced for either disposition; a decision built on top of this module
    only uses evidence for an ``ALLOCATED`` outcome.
    """

    allocation_outcome_id: str
    outcome: PromotedOperationalAllocationOutcome
    research_quantity_ceiling: int
    per_trade_risk_quantity_ceiling: int
    total_open_risk_quantity_ceiling: int
    position_notional_quantity_ceiling: int
    gross_exposure_quantity_ceiling: int
    cash_quantity_ceiling: int
    ask_depth_quantity_ceiling: int
    feasible_quantity: int
    operational_quantity: int
    loss_per_share: Decimal
    reward_per_share: Decimal
    operational_net_reward_risk: Decimal
    binding_ceiling_codes: tuple[str, ...]
    evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._verify()
        object.__setattr__(self, "evidence_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.outcome) is not PromotedOperationalAllocationOutcome:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.outcome.verify_content_identity()
        if self.allocation_outcome_id != self.outcome.allocation_outcome_id:
            raise PromotedOperationalAllocationError(_ERR_EVIDENCE)

        ceilings = _compute_allocation_ceilings(
            self.outcome.quote_outcome,
            self.outcome.portfolio_context,
            self.outcome.allocation_policy,
            self.outcome.state_before,
        )
        for actual, expected in (
            (self.research_quantity_ceiling, ceilings.research_quantity),
            (self.per_trade_risk_quantity_ceiling, ceilings.per_trade_risk_quantity),
            (self.total_open_risk_quantity_ceiling, ceilings.total_open_risk_quantity),
            (self.position_notional_quantity_ceiling, ceilings.position_notional_quantity),
            (self.gross_exposure_quantity_ceiling, ceilings.gross_exposure_quantity),
            (self.cash_quantity_ceiling, ceilings.cash_quantity),
            (self.ask_depth_quantity_ceiling, ceilings.ask_depth_quantity),
            (self.feasible_quantity, ceilings.feasible_quantity),
        ):
            if type(actual) is not int or actual != expected:
                raise PromotedOperationalAllocationError(_ERR_EVIDENCE)
        if (
            type(self.operational_quantity) is not int
            or self.operational_quantity != self.outcome.operational_quantity
        ):
            raise PromotedOperationalAllocationError(_ERR_EVIDENCE)
        for actual, expected in (
            (self.loss_per_share, ceilings.loss_per_share),
            (self.reward_per_share, ceilings.reward_per_share),
            (self.operational_net_reward_risk, ceilings.net_reward_risk),
        ):
            if type(actual) is not Decimal or actual != expected:
                raise PromotedOperationalAllocationError(_ERR_EVIDENCE)
        if self.operational_net_reward_risk != self.outcome.operational_net_reward_risk:
            raise PromotedOperationalAllocationError(_ERR_EVIDENCE)
        expected_codes = _binding_ceiling_codes(ceilings)
        if (
            type(self.binding_ceiling_codes) is not tuple
            or self.binding_ceiling_codes != expected_codes
        ):
            raise PromotedOperationalAllocationError(_ERR_EVIDENCE)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_ALLOCATION_EVIDENCE_SCHEMA_VERSION,
                **{
                    value.name: getattr(self, value.name)
                    for value in fields(self)
                    if value.name != "evidence_id"
                },
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.evidence_id != self._calculated_id():
            raise PromotedOperationalAllocationError(_ERR_EVIDENCE)


def build_promoted_operational_allocation_evidence(
    outcome: PromotedOperationalAllocationOutcome,
) -> PromotedOperationalAllocationEvidence:
    """Pure builder: derive one exact ``PromotedOperationalAllocationEvidence``
    from one already-verified ``PromotedOperationalAllocationOutcome``,
    through the identical calculation path allocation itself used. Never
    changes an existing allocation public type, schema, ID, quantity, or
    reason.
    """

    if type(outcome) is not PromotedOperationalAllocationOutcome:
        raise PromotedOperationalAllocationError(_ERR_TYPE)
    outcome.verify_content_identity()
    ceilings = _compute_allocation_ceilings(
        outcome.quote_outcome,
        outcome.portfolio_context,
        outcome.allocation_policy,
        outcome.state_before,
    )
    return PromotedOperationalAllocationEvidence(
        allocation_outcome_id=outcome.allocation_outcome_id,
        outcome=outcome,
        research_quantity_ceiling=ceilings.research_quantity,
        per_trade_risk_quantity_ceiling=ceilings.per_trade_risk_quantity,
        total_open_risk_quantity_ceiling=ceilings.total_open_risk_quantity,
        position_notional_quantity_ceiling=ceilings.position_notional_quantity,
        gross_exposure_quantity_ceiling=ceilings.gross_exposure_quantity,
        cash_quantity_ceiling=ceilings.cash_quantity,
        ask_depth_quantity_ceiling=ceilings.ask_depth_quantity,
        feasible_quantity=ceilings.feasible_quantity,
        operational_quantity=outcome.operational_quantity,
        loss_per_share=ceilings.loss_per_share,
        reward_per_share=ceilings.reward_per_share,
        operational_net_reward_risk=ceilings.net_reward_risk,
        binding_ceiling_codes=_binding_ceiling_codes(ceilings),
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedOperationalAllocationBatch:
    """Exact, content-addressed allocation result for one promoted
    operational quote-gate batch.

    Every quote-gate PASS outcome, in exact existing order, receives one
    allocation outcome; every quote-gate VETO outcome is preserved
    separately and exactly. A zero-PASS batch is valid: allocation
    outcomes are empty, counts are zero, and ``final_state`` equals
    ``initial_state``.
    """

    quote_gate_batch: VerifiedPromotedOperationalQuoteGateBatch
    portfolio_context: PromotedOperationalPortfolioContext
    allocation_policy: PromotedOperationalAllocationPolicy
    allocation_outcomes: tuple[PromotedOperationalAllocationOutcome, ...]
    upstream_quote_vetoes: tuple[PromotedOperationalQuoteOutcome, ...]
    initial_state: PromotedOperationalAllocationState
    final_state: PromotedOperationalAllocationState
    allocated_count: int
    veto_count: int
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    allocation_batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value in (self.allocated_count, self.veto_count):
            if type(value) is not int or value < 0:
                raise PromotedOperationalAllocationError(_ERR_COVERAGE)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalAllocationError(_ERR_AUTHORITY)
        self._verify_coverage()
        object.__setattr__(self, "allocation_batch_id", self._calculated_id())

    def _verify_coverage(self) -> None:
        if type(self.quote_gate_batch) is not VerifiedPromotedOperationalQuoteGateBatch:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.quote_gate_batch.verify_content_identity()
        if type(self.portfolio_context) is not PromotedOperationalPortfolioContext:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.portfolio_context.verify_content_identity()
        if type(self.allocation_policy) is not PromotedOperationalAllocationPolicy:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.allocation_policy.verify_content_identity()

        portfolio = self.portfolio_context.portfolio
        if portfolio.as_of > self.quote_gate_batch.evaluated_at:
            raise PromotedOperationalAllocationError(_ERR_PORTFOLIO)
        age_seconds = (self.quote_gate_batch.evaluated_at - portfolio.as_of).total_seconds()
        if age_seconds > self.allocation_policy.maximum_portfolio_age_seconds:
            raise PromotedOperationalAllocationError(_ERR_PORTFOLIO)

        pass_outcomes = tuple(
            value
            for value in self.quote_gate_batch.outcomes
            if value.disposition is SwingQuoteGateDisposition.PASS
        )
        veto_outcomes = tuple(
            value
            for value in self.quote_gate_batch.outcomes
            if value.disposition is SwingQuoteGateDisposition.VETO
        )
        if (
            type(self.upstream_quote_vetoes) is not tuple
            or self.upstream_quote_vetoes != veto_outcomes
        ):
            raise PromotedOperationalAllocationError(_ERR_COVERAGE)
        if type(self.allocation_outcomes) is not tuple or any(
            type(value) is not PromotedOperationalAllocationOutcome
            for value in self.allocation_outcomes
        ):
            raise PromotedOperationalAllocationError(_ERR_COVERAGE)
        if len(self.allocation_outcomes) != len(pass_outcomes):
            raise PromotedOperationalAllocationError(_ERR_COVERAGE)

        if type(self.initial_state) is not PromotedOperationalAllocationState:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.initial_state.verify_content_identity()
        expected_initial_state = PromotedOperationalAllocationState(
            cash_available=portfolio.cash_available,
            gross_exposure=portfolio.gross_exposure,
            open_risk=portfolio.open_risk,
            open_listing_keys=self.portfolio_context.open_listing_keys,
        )
        if self.initial_state.state_id != expected_initial_state.state_id:
            raise PromotedOperationalAllocationError(_ERR_COVERAGE)

        expected_state = self.initial_state
        for outcome, pass_outcome in zip(
            self.allocation_outcomes, pass_outcomes, strict=True
        ):
            outcome.verify_content_identity()
            if outcome.quote_outcome.outcome_id != pass_outcome.outcome_id:
                raise PromotedOperationalAllocationError(_ERR_COVERAGE)
            if outcome.portfolio_context.context_id != self.portfolio_context.context_id:
                raise PromotedOperationalAllocationError(_ERR_COVERAGE)
            if (
                outcome.allocation_policy.allocation_policy_id
                != self.allocation_policy.allocation_policy_id
            ):
                raise PromotedOperationalAllocationError(_ERR_COVERAGE)
            if outcome.state_before.state_id != expected_state.state_id:
                raise PromotedOperationalAllocationError(_ERR_COVERAGE)
            expected_state = outcome.state_after

        if type(self.final_state) is not PromotedOperationalAllocationState:
            raise PromotedOperationalAllocationError(_ERR_TYPE)
        self.final_state.verify_content_identity()
        if self.final_state.state_id != expected_state.state_id:
            raise PromotedOperationalAllocationError(_ERR_COVERAGE)

        allocated = sum(
            1
            for value in self.allocation_outcomes
            if value.disposition is PromotedOperationalAllocationDisposition.ALLOCATED
        )
        vetoed = len(self.allocation_outcomes) - allocated
        if self.allocated_count != allocated or self.veto_count != vetoed:
            raise PromotedOperationalAllocationError(_ERR_COVERAGE)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_ALLOCATION_BATCH_SCHEMA_VERSION,
                **{
                    value.name: getattr(self, value.name)
                    for value in fields(self)
                    if value.name != "allocation_batch_id"
                },
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify_coverage()
        if self.allocation_batch_id != self._calculated_id():
            raise PromotedOperationalAllocationError(_ERR_COVERAGE)


def assemble_promoted_operational_allocation_batch(
    *,
    quote_gate_batch: VerifiedPromotedOperationalQuoteGateBatch,
    portfolio_context: PromotedOperationalPortfolioContext,
    allocation_policy: PromotedOperationalAllocationPolicy,
) -> VerifiedPromotedOperationalAllocationBatch:
    """Deterministically allocate every quote-gate PASS outcome, in exact
    existing order, under the exact injected portfolio context and policy.

    Never discovers a portfolio, never reranks by symbol/spread/score/
    quantity, and never grants notification or execution authority.
    """

    if type(quote_gate_batch) is not VerifiedPromotedOperationalQuoteGateBatch:
        raise PromotedOperationalAllocationError(_ERR_TYPE)
    quote_gate_batch.verify_content_identity()
    if type(portfolio_context) is not PromotedOperationalPortfolioContext:
        raise PromotedOperationalAllocationError(_ERR_TYPE)
    portfolio_context.verify_content_identity()
    if type(allocation_policy) is not PromotedOperationalAllocationPolicy:
        raise PromotedOperationalAllocationError(_ERR_TYPE)
    allocation_policy.verify_content_identity()

    portfolio = portfolio_context.portfolio
    if portfolio.as_of > quote_gate_batch.evaluated_at:
        raise PromotedOperationalAllocationError(_ERR_PORTFOLIO)
    age_seconds = (quote_gate_batch.evaluated_at - portfolio.as_of).total_seconds()
    if age_seconds > allocation_policy.maximum_portfolio_age_seconds:
        raise PromotedOperationalAllocationError(_ERR_PORTFOLIO)

    pass_outcomes = tuple(
        value
        for value in quote_gate_batch.outcomes
        if value.disposition is SwingQuoteGateDisposition.PASS
    )
    veto_outcomes = tuple(
        value
        for value in quote_gate_batch.outcomes
        if value.disposition is SwingQuoteGateDisposition.VETO
    )

    initial_state = PromotedOperationalAllocationState(
        cash_available=portfolio.cash_available,
        gross_exposure=portfolio.gross_exposure,
        open_risk=portfolio.open_risk,
        open_listing_keys=portfolio_context.open_listing_keys,
    )

    state = initial_state
    outcomes: list[PromotedOperationalAllocationOutcome] = []
    for pass_outcome in pass_outcomes:
        try:
            replayed = _evaluate_allocation(
                pass_outcome, portfolio_context, allocation_policy, state
            )
        except PromotedOperationalAllocationError:
            raise
        except Exception:
            raise PromotedOperationalAllocationError(_ERR_REPLAY) from None
        (
            disposition,
            reasons,
            quantity,
            notional,
            cost,
            loss,
            net_reward_risk,
            state_after,
        ) = replayed
        outcome = PromotedOperationalAllocationOutcome(
            quote_outcome=pass_outcome,
            portfolio_context=portfolio_context,
            allocation_policy=allocation_policy,
            state_before=state,
            state_after=state_after,
            disposition=disposition,
            reason_codes=reasons,
            operational_quantity=quantity,
            reference_entry_price=pass_outcome.reference_entry_price,
            entry_notional=notional,
            estimated_round_trip_cost=cost,
            planned_max_loss=loss,
            operational_net_reward_risk=net_reward_risk,
        )
        outcomes.append(outcome)
        state = state_after

    allocated = sum(
        1
        for value in outcomes
        if value.disposition is PromotedOperationalAllocationDisposition.ALLOCATED
    )
    return VerifiedPromotedOperationalAllocationBatch(
        quote_gate_batch=quote_gate_batch,
        portfolio_context=portfolio_context,
        allocation_policy=allocation_policy,
        allocation_outcomes=tuple(outcomes),
        upstream_quote_vetoes=veto_outcomes,
        initial_state=initial_state,
        final_state=state,
        allocated_count=allocated,
        veto_count=len(outcomes) - allocated,
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )
