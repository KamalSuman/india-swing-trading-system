# Promoted operational paper decision

This is the fourth and final pure boundary in the research-to-paper-operation
seam before a runner exists: a deterministic decision layer that consumes one
already-verified `VerifiedPromotedOperationalAllocationBatch` and produces
exactly one content-addressed `PAPER_BUY` or `NO_TRADE` decision package with
complete quantity-cap evidence, rationale, cancellation conditions, veto
coverage, and a human-readable advisory. It performs no I/O, environment,
clock, network, broker, filesystem, GCP, notification, or persistence access,
and it cannot place an order, send a notification, or register a paper
trade.

## Allocation evidence (in `promoted_operational_allocation.py`)

`PromotedOperationalAllocationEvidence` is an additive extension of the
allocation module -- it does not change any existing allocation public type,
schema, ID, quantity, or reason. It binds the exact `allocation_outcome_id`
of one `PromotedOperationalAllocationOutcome` and replays every independently
derived quantity ceiling (research, per-trade risk, remaining total open
risk, position notional, remaining gross exposure, cash including cost, and
top-ask-depth participation), the feasible quantity before any non-quantity
veto, the operational quantity, loss/reward per share, operational net
reward/risk, and the exact sorted set of ceiling codes that actually bound
the feasible quantity (more than one can tie).

Allocation's own private `_evaluate_allocation` and the evidence builder
`build_promoted_operational_allocation_evidence` share one calculation path,
`_compute_allocation_ceilings` -- evidence can never diverge from the actual
allocation decision. The refactor is exact: allocation originally capped
quantity with one combined `floor(min(per_trade_budget, remaining_open_risk)
/ loss_per_share)` risk term; because `floor(min(a, b) / c) == min(floor(a /
c), floor(b / c))` for positive `c`, splitting that into two separate
per-trade-risk and total-open-risk ceilings and taking the seven-way minimum
across every ceiling reproduces byte-identical feasible quantities, reason
codes, and outcome IDs.

## New types (in `promoted_operational_decision.py`)

- `PromotedOperationalTradeRecommendation` binds exactly one `ALLOCATED`
  allocation outcome and its exact evidence, plus deterministically
  replayed `rationale` and `cancellation_conditions` tuples. It never
  changes a price, quantity, tick, cost buffer, or holding period retained
  inside the outcome. `execution_eligible` and `research_only` are read-only
  properties here, not authority-bearing fields, matching the outcome-level
  convention already used by `PromotedOperationalAllocationOutcome`.
- `PromotedOperationalDailyDecision` is the singular decision for one exact
  allocation batch. Zero `ALLOCATED` outcomes yields `NO_TRADE` with
  `recommendation=None`; exactly one yields `PAPER_BUY` with one
  recommendation. More than one `ALLOCATED` outcome is rejected as an
  integrity error, checked unconditionally before any action/recommendation
  shape check, even if an upstream policy permitted it. `evaluated_at` must
  equal the exact quote-gate batch `evaluated_at` in UTC-canonical form, and
  `target_session` must equal the preparation's target session.
  `paper_only`/`notification_eligible`/`execution_eligible` are retained
  fixed fields (always `True`/`False`/`False`), matching the aggregate-level
  convention already used by `VerifiedPromotedOperationalAllocationBatch`.
- `PromotedOperationalDecisionPackage` retains the exact decision, the
  rendered advisory text, its SHA-256, and the same fixed authority flags.
  The text and hash are always replayed from the retained decision, never
  trusted from supplied prose, and are bound to 128 KiB of safe UTF-8 text.

## Veto diagnostics

Every upstream quote-gate `VETO` is rendered as `QUOTE:<listing_key>:<reason>`
and every allocation `VETO` as `ALLOCATION:<listing_key>:<reason>`, collected
into one sorted, unique, canonical set on the decision. No veto is ever
silently discarded, whether the batch produced a `PAPER_BUY` alongside other
vetoed candidates or a `NO_TRADE` with none allocated at all.

## Rationale and cancellations

Rationale deterministically states: that upstream research selection is
comparative and never a probability or confidence estimate; that quote
freshness/depth/spread/circuit checks passed at the exact evaluated time;
every quantity ceiling and which one(s) actually bound the feasible
quantity; the portfolio/policy IDs and before/after cash, exposure, and open
risk; the planned entry notional, cost, loss, and net reward/risk; and every
relevant lineage ID down to the allocation evidence itself. Cancellation
conditions require re-evaluation if: the decision window has passed; the
quote snapshot, depth, spread, or circuit state has changed; the best ask
has exceeded the research limit or reached the stop/target; the portfolio
artifact/context or allocation policy has changed; the listing has become an
already-open position; or manual delay, slippage, or gap risk could exceed
the planned maximum loss.

## Renderer

`render_promoted_operational_decision` always opens with exactly:

```text
PAPER RESEARCH ONLY — MANUAL REVIEW REQUIRED — DO NOT AUTO-EXECUTE
```

followed by the action, target session, decision time, portfolio/policy
IDs, complete trade levels and sizing for `PAPER_BUY`, the rationale,
cancellation conditions, full lineage, every veto diagnostic (or `NONE`),
the decision ID, and an explicit statement that the package cannot place an
order, notify, or grant execution authority.

## No probability, confidence, or authority override

No public field on any type in this boundary is named `probability` or
`confidence`, and no score is ever described as one. `PAPER_BUY` is a paper
advisory label only -- it is never the unqualified `BUY` action, and no field
anywhere in this boundary can widen `paper_only`/`notification_eligible`/
`execution_eligible` beyond their fixed values.

## Non-goals

This module does not acquire a quote, load a portfolio, persist terminal
state, send a Telegram message, or register a paper trade. Those, plus the
operational runner that composes this entire seam end to end, remain a later
milestone.
