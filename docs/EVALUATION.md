# Leakage-safe evaluation boundary

The `india_swing.evaluation` package establishes immutable chronological folds
and create-once trial preregistrations before any strategy, Kronos model,
TradingAgents component, or benchmark can be evaluated. It does not yet
calculate returns or claim strategy performance.

## Implemented split contract

`build_expanding_purged_walk_forward_plan` accepts an explicit ordered tuple of
NSE trading sessions and constructs expanding training folds with separate
validation and test windows. It enforces:

- chronological, contiguous partitions in trading-session time;
- a forward-label horizon of at least ten sessions for the initial swing model;
- an embargo at least as long as the maximum label horizon between training and
  validation and again between validation and test;
- no repeated test session across folds;
- expanding training coverage and strictly advancing test windows;
- one pinned calendar version and content-derived IDs for every fold and plan;
- rejection of random/k-fold cross-validation for time-series trials.

Weekend or holiday distance is irrelevant. Purges and horizons are calculated
from positions in the supplied versioned session tuple, never from calendar-day
subtraction.

## What this does not authorize

A valid split plan or registration alone is not a reportable backtest.
Evaluation must still be blocked until the following are implemented and
supplied:

- point-in-time universe and stable listing identity for every historical date;
- mature forward labels separated from feature access;
- an effective-dated Indian equity cost schedule and stressed slippage case;
- conservative order, circuit, suspension, missing-quote, and delisting rules;
- identical fills/costs for the strategy and simple benchmark;
- append-only outcomes including failed, negative, and invalidated trials.

## Trial preregistration

`TrialRegistration` freezes the hypothesis, exploratory/confirmatory stage,
strategy family and parent, evaluation dates, universe/data/split IDs, label
horizon, benchmark, metrics, model/code/dependency/configuration hashes,
exclusions, risk/cost/execution bindings, slippage scenarios, thresholds,
multiple-testing policy, seed/repetition protocol, and sealed holdout ID.

Confirmatory registrations require a sealed holdout and stressed slippage above
the nonzero base assumption. A changed configuration gets a new content-derived
trial ID. `LocalTrialRegistry` requires later variants in the same strategy
family to link to an already registered parent, publishes one create-once JSON
record, detects tampering, and keeps all family registrations queryable. Its
default root is `var/trial_registry`.

Holdout access/unsealing and trial outcomes are intentionally not fields that
can mutate this registration. They remain to be implemented as separate
append-only events.

The current real price archive contains only one session and remains
`COLLECTION_ONLY`. It cannot be passed off as evaluation data.
