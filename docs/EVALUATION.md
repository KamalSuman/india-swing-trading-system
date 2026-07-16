# Leakage-safe evaluation boundary

The `india_swing.evaluation` package establishes immutable chronological folds
and create-once trial preregistrations before any strategy, Kronos model,
TradingAgents component, or benchmark can be evaluated. Separate append-only
lifecycle events audit holdout access and terminal outcomes. The package now
calculates content-bound trial metrics from simulated fills and costs. The real
archive remains collection-only, so this does not claim strategy performance.

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
- historical cost schedules for every evaluated date and a stressed slippage case;
- complete suspension, partial-fill, missing-quote, delisting, and same-day
  intraday-cost rules;
- identical fills/costs for the strategy and simple benchmark;
- a create-once store for full generated evaluation-result evidence.

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
can mutate this registration.

## Lifecycle event chain

`LocalTrialLifecycleStore` writes create-once, per-trial event files linked by
sequence number and predecessor content ID. A trial must be registered before
its first `TRIAL_STARTED` event. The chain records holdout unsealing and
feature/label/result access, then preserves completed, failed, aborted, and
invalidated states.

Holdout access fails before an unseal event or when its ID disagrees with the
sealed registration. A completed confirmatory trial requires audited holdout
results access and every registered metric. Completion no longer accepts
caller-provided metric tuples: it requires a `TrialEvaluationResult`, verifies
its trial/split/execution/cost/threshold bindings, and records the generated
result ID. `passed=false` is a first-class
terminal result and remains queryable. A later audit can append invalidation but
cannot replace the original result. After a parent's holdout is unsealed, a
confirmatory successor cannot reuse that holdout; it needs a new sealed holdout
or must be exploratory.

The local predecessor chain detects mutation, reordered events, and missing
interior events. As with the other local stores, a filesystem administrator can
still delete the newest files and related evidence. Production therefore needs
conditional immutable Cloud Storage writes, retention controls, and access
logs.

## Engine-generated evaluation

`TrialEvaluationEngine` consumes an immutable trial registration, its exact
`PurgedWalkForwardPlan`, a content-derived evaluation dataset, ordered trade
intents, the registered daily execution policy, the registered effective-dated
cost schedule, and initial capital. It rejects any mismatched content ID or
version.

Only signal and entry sessions inside a preregistered test fold are evaluated.
The engine rejects signals from training/validation partitions and rejects a
trade whose realized entry or exit crosses its test-fold boundary. The dataset
calendar must exactly match the split plan's versioned ordered session tuple.

For each eligible intent, the engine generates entry and exit fills, applies
itemized contract-day charges, marks open holdings at each session close, and
produces net profit, net return, annualized net CAGR, mark-to-market maximum
drawdown, two-sided turnover, and executed trade count.

Pass/fail is recomputed from preregistered thresholds. Result construction
checks final equity against gross fills less charges, recomputes drawdowns and
metrics, and binds all evidence into a 64-character result ID. Post-calculation
mutation invalidates that identity.

Synthetic trials require an explicitly synthetic dataset. A non-synthetic
trial requires `POINT_IN_TIME_VERIFIED`; `COLLECTION_ONLY` data fails before any
metric is returned. Missing bars, insufficient horizon coverage, an unfilled
time exit, a sell-side circuit lock at the horizon, or a same-day round trip
that the delivery schedule cannot price also fail instead of being silently
skipped.

The current real price archive contains only one session and remains
`COLLECTION_ONLY`. It cannot be passed off as evaluation data.
