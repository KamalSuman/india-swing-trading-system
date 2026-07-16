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
- complete suspension, partial-fill, missing-quote, delisting, and partially
  netted-order allocation rules;
- point-in-time verified inputs for the implemented deterministic strategy and
  benchmark generators;
- multiple-testing correction over actual trial families and repetitions.

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
caller-provided metric tuples: it requires a
`TrialEvaluationComparisonResult`, verifies its
trial/split/execution/cost/threshold/benchmark bindings, requires the comparison
and every referenced full scenario artifact to already exist, and records the
comparison ID. `passed=false` is a first-class
terminal result and remains queryable. A later audit can append invalidation but
cannot replace the original result. After a parent's holdout is unsealed, a
confirmatory successor cannot reuse that holdout; it needs a new sealed holdout
or must be exploratory.

`TRIAL_PROMOTED` is a separate post-completion research event. It requires a
configured family-aggregate store, an eligible current-family decision, and the
exact comparison ID already recorded by `TRIAL_COMPLETED`. Only one promotion
may follow completion; invalidation remains append-only afterward. Promotion
does not authorize a live alert or change any dataset readiness state.

The local predecessor chain detects mutation, reordered events, and missing
interior events. As with the other local stores, a filesystem administrator can
still delete the newest files and related evidence. Production therefore needs
conditional immutable Cloud Storage writes, retention controls, and access
logs.

## Sealed dataset assembly

`assemble_evaluation_dataset` is the admission boundary between historical
collection and the evaluation engine. It consumes one exact as-of calendar
vintage and one exact universe snapshot for every evaluation session, one
normalized price session for every session, and effective-dated tick-size specifications. It
produces the existing `EvaluationDataset`, point-in-time baseline instruments,
and a content-bound `EvaluationSessionEvidence` record for every session.

Admission fails unless all inputs share either the explicit synthetic-test
readiness or `POINT_IN_TIME_VERIFIED`. `COLLECTION_ONLY` fails. Sessions must be
gap-free relative to each preceding as-of calendar; each universe must bind its
same-session calendar vintage; every raw listing key must exist in that session's broad universe;
and every actionable listing must have either an identity-matched bar or an
exact explicit nontrading listing record. Absence from a Bhavcopy is therefore
never treated as an eligibility or delisting fact.

Each calendar vintage must itself have been sealed by that session's decision
cutoff. The vintage used for session D must identify the next supplied session
as its then-known next exchange session, and that next-session record must also
have been known by the cutoff. A calendar assembled later in the evaluation
window therefore cannot be reused for earlier decisions merely because its
historical dates look correct.

Every actionable listing requires a stable instrument ID, listing ID, ISIN, and
exactly one timely effective tick size. The generated baseline instrument now
retains the stable identity and a `(session, universe_snapshot_id)` binding for
every eligible session. The current baseline deliberately rejects symbol,
listing, ISIN, or tick-size transitions rather than joining them incorrectly.
A later model can support transitions only by making their execution semantics
explicit.

`point_in_time_price_session_from_nse` normalizes a replay-verified raw NSE EOD
artifact but preserves its readiness and `actionable` flag. Consequently the
present manually acquired price archive remains ineligible. It also cannot
invent explicit nontrading evidence or circuit-lock facts absent from the raw
source.

`LocalEvaluationDatasetStore` publishes one create-once JSON artifact under the
evaluation evidence root. Reads reconstruct the dataset, bars, instruments,
daily bindings, raw-price source references, effective tick specifications, and
nested content IDs. This local materialization is content-addressed;
point-in-time trust still comes from the referenced upstream stores and their
future verified acquisition/adjudication paths.

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

`LocalTrialEvaluationResultStore` publishes the complete fills, itemized charge
legs, mark-to-market equity curve, metrics, thresholds, and identities as one
create-once JSON artifact. Reads reconstruct every nested typed value and
recompute fill, trade, charge, metric, pass/fail, and result identities. A
lifecycle completion cannot reference an in-memory-only result.

`TrialEvaluationComparisonEngine` runs strategy and benchmark intents through
the exact same dataset, split plan, capital, execution policy, and cost schedule.
The execution-policy identity freezes both base and optional stressed slippage.
If stress is registered, the engine generates four evidence results: strategy
base/stressed and benchmark base/stressed. The comparison passes only when the
strategy clears its preregistered thresholds and its primary metric is at least
the benchmark value in every required scenario.

`LocalTrialEvaluationComparisonStore` first publishes every referenced full
result, then writes a create-once comparison artifact containing their IDs,
registered strategy/benchmark/slippage bindings, excess-primary metrics, and
the recomputed pass result. Lifecycle completion consumes this persisted
comparison, not a selectively chosen scenario.

## Deterministic baseline generators

`DeterministicMomentumIntentGenerator` implements a deliberately simple,
non-LLM cross-sectional close-momentum baseline. On the first test session of
each registered fold it ranks only instruments explicitly eligible in their
point-in-time universe record. Momentum uses the signal close and one
preregistered historical close; future bars, validation labels, and test
outcomes are not inputs. Selected limit orders become eligible on the next
session only.

`DeterministicEqualWeightBenchmarkGenerator` selects the most liquid eligible
constituents using signal-close turnover, then assigns equal slot notional. It
is a tradable holding-window comparator, not an index-return shortcut. Both
generator configurations have content-derived IDs that must exactly equal the
trial's registered model and benchmark IDs.

Every instrument receives a content-bound `GeneratedSignalDecision`, including
its score, selected/veto reason, and the exact bar IDs used as evidence.
`PointInTimeInstrument.eligible_sessions` prevents a later constituent list
from entering an earlier fold. `DeterministicBaselineEvaluationEngine` creates
strategy and benchmark batches across all registered folds and sends both to
the existing base/stressed comparison engine.

`LocalGeneratedIntentBatchStore` permits exactly one create-once strategy batch
and one create-once benchmark batch for each registered trial. Its path is
fixed by trial and role, rather than by caller-selected batch ID, so a later
result-informed rerun cannot coexist as another candidate. Reads reconstruct
every decision, entry order, and intent and recheck all nested content IDs.
`LocalDeterministicComparisonRunStore` publishes both batches before the
comparison and verifies every executed trade came from the corresponding
generated intent set.

Each deterministic run also contains a `FoldComparisonSummary` recomputed from
its persisted intent lineage and mark-to-market equity curves. Fold return,
CAGR, drawdown, profit, turnover, and trade count start from that scenario's
own fold-opening equity. Base and stressed excess-primary metrics are reported
separately; per-fold outperformance is descriptive and does not replace the
trial's aggregate pass thresholds.

## Multiple-testing family gate

The supported `holm-familywise-primary-fold-sign-v1` policy is fully specified:
for each registered variant, count strictly positive primary-metric excesses
across folds, calculate an exact one-sided sign-test tail separately for base
and stressed execution, and use the worse p-value. Holm step-down at alpha 0.05
then covers every registered trial in the strategy family. Ties are non-wins.
A statistically rejected variant is still ineligible unless its full persisted
comparison also passed all trading thresholds and benchmark gates.

`TrialFamilyEvaluationAggregator` refuses unsupported policy text, missing
family variants, duplicate runs, non-persisted batches/comparisons, or missing
stressed-fold evidence. It derives p-values rather than accepting caller-
supplied confidence. `LocalTrialFamilyAggregateStore` recomputes the aggregate
from persisted runs before publishing one create-once artifact for the exact
registered-family snapshot. If another variant is later registered, the older
snapshot cannot be used for promotion. Non-overlapping folds can still be regime-correlated, so
this exact sign/Holm gate is a preregistered preliminary safeguard, not proof
that fold signs are statistically independent or that a market edge will
persist.

`build_trial_family_evaluation_report` renders a content-bound Markdown report
from the aggregate and exact run set. It includes family decisions, Holm
cutoffs, comparison eligibility, every fold's base/stressed excess, and an
interpretation boundary. `LocalDeterministicComparisonRunStore` now publishes
one create-once run manifest per trial, binding both generated batches, the
comparison, and every fold summary. A second result-derived run for the same
trial is rejected.

`LocalTrialFamilyReportStore` publishes one create-once report for each family
aggregate and rechecks the aggregate and run evidence before writing. The
`india-swing-evaluation` CLI can publish from persisted current-family runs,
list report IDs, or render stored Markdown. It cannot accept caller-authored
metrics, Markdown, aggregate decisions, or a selectively supplied trial list.

Synthetic trials require an explicitly synthetic dataset. A non-synthetic
trial requires `POINT_IN_TIME_VERIFIED`; `COLLECTION_ONLY` data fails before any
metric is returned. Missing bars, insufficient horizon coverage, an unfilled
time exit, or a sell-side circuit lock at the horizon fails instead of being
silently skipped. A fully netted same-day stop is priced with the intraday
tariff; partial same-day netting fails without allocation evidence.

The current real price archive contains only one session and remains
`COLLECTION_ONLY`. It cannot be passed off as evaluation data.
