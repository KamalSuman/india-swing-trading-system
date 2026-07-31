# Promoted operational capital allocation

This is the third bounded increment in the research-to-paper-operation
seam: a pure, deterministic capital allocator that consumes one
already-verified `VerifiedPromotedOperationalQuoteGateBatch` plus an exact,
evidence-bound aggregate portfolio context, and allocates only quote-gate
PASS candidates under current cash, risk, exposure, position,
research-liquidity, and top-of-book constraints. It performs no I/O,
environment, clock, network, broker, filesystem, GCP, notification, or
persistence access, and produces no BUY decision, notification, paper
registration, or execution authority.

## Reused legacy types

`src/india_swing/promoted_operational_allocation.py` reuses the accepted
`SwingPortfolioSnapshot` and `SwingPortfolioSizingPolicy` types from
`src/india_swing/risk/swing_portfolio.py` exactly -- neither type's public
shape, schema, or monetary risk limits are duplicated or changed. Because
those parent types' own `verify_content_identity` only compares hashes, a
self-consistently rehashed but semantically invalid instance (for example a
snapshot with `cash_available + gross_exposure > capital`, or a policy with
`per_trade_risk_fraction` widened past 1) would otherwise pass. Both
promoted wrapper types instead independently reconstruct a fresh exact
parent instance from its own retained fields on every construction and
`verify_content_identity` call, so the parent's `__post_init__` semantic
ceilings run again regardless of what ID accompanies the tampered instance.

## New types

- `PromotedOperationalPortfolioContext` binds one exact
  `SwingPortfolioSnapshot` to an externally supplied
  `source_portfolio_artifact_id` and the exact sorted, unique, canonical
  NSE `open_listing_keys` it holds -- the snapshot itself does not identify
  open symbols, so duplicate-symbol protection is never guessed. The key
  count must exactly equal `portfolio.open_positions`.
- `PromotedOperationalAllocationPolicy` binds one exact
  `SwingPortfolioSizingPolicy` plus a positive
  `maximum_portfolio_age_seconds`. `maximum_portfolio_age_seconds` defaults
  to 300; `paper_only`, `notification_eligible`, and `execution_eligible`
  default to (and are always validated as) `True`, `False`, `False`. An
  explicit caller value for any of the four is still validated identically.
- `PromotedOperationalAllocationState` tracks `cash_available`,
  `gross_exposure`, `open_risk`, and the exact sorted unique
  `open_listing_keys` set. Its open-position count is always
  `len(open_listing_keys)` -- never an independent field that could drift.
- `PromotedOperationalAllocationOutcome` retains one exact PASS
  `PromotedOperationalQuoteOutcome`, the portfolio context, the allocation
  policy, `state_before`/`state_after`, an `ALLOCATED`/`VETO` disposition,
  sorted unique reason codes, `operational_quantity`, the quote's own
  `reference_entry_price`, `entry_notional`, `estimated_round_trip_cost`,
  `planned_max_loss`, and `operational_net_reward_risk`. It replays every
  value from its retained inputs on construction and on
  `verify_content_identity`.
- `VerifiedPromotedOperationalAllocationBatch` retains the exact
  quote-gate batch, portfolio context, policy, the ordered allocation
  outcome for every PASS candidate, the exact preserved quote-gate VETO
  outcomes, `initial_state`/`final_state`, allocated/veto counts, and the
  fixed paper-only/no-notification/no-execution flags. It independently
  replays coverage, ordering, the full state chain, and every outcome.

## Allocation rule

For each retained research intent: `loss_per_share = reference_entry_price
- stop_price + estimated_cost_buffer` and `reward_per_share = target_price
- reference_entry_price - estimated_cost_buffer` (both must be positive, an
integrity requirement, not a veto). `operational_quantity` is the minimum
of: the retained research quantity; the per-trade risk budget quantity; the
remaining total-open-risk quantity; the maximum position-notional
quantity; the remaining gross-exposure quantity; the cash quantity
(including the cost buffer); and the top-ask-depth participation quantity
-- so it can never exceed the research quantity or the available top-ask
depth. A duplicate open listing, a research order whose
`maximum_participation` is wider than
`policy.maximum_daily_turnover_participation` (there is no historical
volume in a live quote snapshot to size against, so this is vetoed rather
than silently recalculated), a breached reward/risk minimum, a
daily-loss/pilot-drawdown halt, a reached open-position or
new-positions-per-run cap, or any exhausted capacity cap yields `VETO` with
`operational_quantity`/`entry_notional`/`estimated_round_trip_cost`/
`planned_max_loss` all zero and `state_after` exactly `state_before`.
`operational_net_reward_risk` and `reference_entry_price` are still
populated for a `VETO` -- they describe the quote itself, not an allocation
amount. Quote-gate PASS outcomes are processed strictly in their existing
order and chained sequentially, so an earlier allocation's consumed
capacity (including its newly opened listing key) is visible to every
later candidate in the same batch; nothing is ever reranked by symbol,
spread, model score, or quantity.

## Portfolio integrity

A portfolio snapshot whose `as_of` is after the quote-gate batch's
`evaluated_at`, or older than `maximum_portfolio_age_seconds`, is a batch
integrity failure -- raised before any candidate is evaluated, never a
per-candidate veto.

## Context-independent Decimal arithmetic

All allocation arithmetic runs inside an explicit `decimal.localcontext()`
with a fixed precision and rounding mode, so the result never depends on
the caller's ambient global `decimal` context. `KiteFullQuote.mid_price`
and `spread_bps` (`src/india_swing/market_data/models.py`) are isolated the
same way, at the established precision-28/`ROUND_HALF_EVEN` normal context,
so a `VerifiedPromotedOperationalQuoteGateBatch` -- and therefore any
allocation batch built from it -- replays identically regardless of the
caller's ambient precision.

## Non-goals

This module does not acquire a quote, load a portfolio, persist terminal
state, send a Telegram message, register a paper trade, or produce a
human-facing paper decision. Quote acquisition, portfolio loading, terminal
state, notification, and paper-ledger registration remain later
milestones.
