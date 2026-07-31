# Promoted operational runner core

`src/india_swing/promoted_operational_runner.py` is the orchestration core
for the promoted paper-operation seam: it binds one exact quote-gate
specification, allocation policy, expected quote/portfolio source identity,
and a Kite-safe quote-chunk ceiling into one `PromotedOperationalRunSpec`,
acquires only exact read-only inputs through two injected ports plus an
injected clock, and runs the existing pure
`evaluate_promoted_operational_quote_gate` ->
`assemble_promoted_operational_allocation_batch` ->
`assemble_promoted_operational_decision_package` chain exactly once in
order. It produces one content-addressed `COMPLETE` or sanitized `FAILED`
`PromotedOperationalRunResult` and nothing else: no persistence,
notification, broker order, paper-ledger registration, environment/
credential read, GCP, or network-client construction exists anywhere in
this module.

## Injected ports

`PromotedOperationalQuoteSource` exposes only `source_id` and
`fetch_full_quotes(exact_listing_keys)`. `PromotedOperationalPortfolioSource`
exposes only `source_id` and `read_portfolio_context()`, returning one exact
`PromotedOperationalPortfolioContext` -- the runner never infers open
listings or synthesizes portfolio evidence itself; that binding is entirely
the source's own responsibility. Source properties are themselves untrusted
capabilities, exactly like the acquisition methods, so neither `source_id`
property is ever read until *after* the initial time is confirmed to fall
inside the decision window (see Timing below). Once in-window, both source
identities are read and validated against the spec's pinned
`expected_quote_source_id`/`expected_portfolio_source_id` before either
acquisition method is ever called; a malformed, unreadable, or mismatched
identity fails closed with `SOURCE_IDENTITY_INVALID` and touches neither
acquisition method. `START_BEFORE_WINDOW`, `START_AFTER_DEADLINE`, and
`SOURCE_IDENTITY_INVALID` results retain `evaluated_at=None` and no
artifacts; the first two also retain `quote_source_id`/`portfolio_source_id`
as `None`, since neither property was ever touched.

## Timing

The initial clock read (`started_at`) is the one case that raises
`PromotedOperationalRunnerError` directly rather than returning a result:
if the very first value isn't an exact timezone-aware `datetime`, there is
no trustworthy timestamp to build a terminal artifact from. Every later
clock read is caught and becomes a sanitized `FAILED` result instead.
`started_at` is checked against `[decision_not_before, decision_deadline]`
immediately after that first read -- before either source_id property, and
therefore strictly before any acquisition. `evaluated_at` is read only
after both quote and portfolio acquisition, must not precede `started_at`,
the acquired quote batch's `observed_at`, or the portfolio's own `as_of`,
and must not exceed the decision deadline. `completed_at` is read after
decision assembly and must not precede `evaluated_at` or exceed the
deadline. All three are always retained in UTC-canonical form, and
`PromotedOperationalRunResult.verify_content_identity` independently
re-checks `started_at` against the exact decision window on every replay --
a self-consistently rehashed `COMPLETE` result whose `started_at` precedes
`decision_not_before` or follows `decision_deadline` still fails, even when
every other artifact and ID is otherwise valid.

A `CLOCK_NON_MONOTONIC` result is the one case where the retained
`evaluated_at` may legitimately violate ordering against `started_at` or an
acquired artifact's timestamp -- that is exactly the violation being
recorded for audit, so `PromotedOperationalRunResult.verify_content_identity`
does not re-reject it when that failure code is present. Every other
ordering invariant (`completed_at` never before `started_at` or
`evaluated_at`, and the same "future artifact" check when no clock
violation is recorded) is still enforced unconditionally.

## Deterministic quote chunking

For a nonempty preparation, the runner sorts the complete preparation
listing-key set, splits it into deterministic contiguous chunks no larger
than `maximum_quote_chunk_size` (bounded to `[1, 500]`, matching Kite's
per-request full-quote limit), and calls the quote source once per chunk.
Each returned `FullQuoteBatch` is independently type-checked, content-
verified, and required to cover exactly its requested chunk; every chunk
must share one `provider_version`. The aggregate retains the earliest
`requested_at`, the latest `observed_at`, and quotes in exact sorted
transport order. Any malformed object, wrong type, missing/extra/reordered
key, or inconsistent provider version raises internally and is reported as
`QUOTE_COVERAGE_INVALID`, distinct from a raw exception the source itself
raised (`QUOTE_ACQUISITION_FAILED`) -- neither ever reaches the quote gate.

## Zero-candidate handling

For a zero-candidate preparation the runner never calls
`fetch_full_quotes` and retains `quote_batch=None`. The portfolio context
is still read and independently verified in both the zero and nonzero
cases -- after quote acquisition (or the zero-candidate bypass) -- and its
own `source_portfolio_artifact_id` must equal the spec's pinned
`expected_portfolio_source_id`, a separate check from the portfolio
*source's* own pinned identity. A zero-candidate run can still reach
`COMPLETE` with a deterministic `NO_TRADE` decision.

## Sanitized failure coverage

`PromotedOperationalRunFailureCode` covers: `START_BEFORE_WINDOW`,
`START_AFTER_DEADLINE`, `SOURCE_IDENTITY_INVALID`,
`QUOTE_ACQUISITION_FAILED`, `QUOTE_COVERAGE_INVALID`,
`PORTFOLIO_ACQUISITION_FAILED`, `CLOCK_NON_MONOTONIC`,
`EVALUATION_AFTER_DEADLINE`, `QUOTE_GATE_FAILED`, `ALLOCATION_FAILED`
(including a stale or future portfolio -- a *future* portfolio is normally
caught earlier by the runner's own monotonic-clock check against
`evaluated_at`, while *staleness* beyond the allocation policy's own
`maximum_portfolio_age_seconds` is caught here, inside allocation itself),
`DECISION_ASSEMBLY_FAILED` (including more than one `ALLOCATED` outcome,
which is rejected as an integrity error even if an upstream allocation
policy technically permitted it), and `COMPLETION_AFTER_DEADLINE`. No
result ever retains exception text, credentials, URLs, payload reprs, or a
source-provided message -- only these canonical, sorted, unique codes.

## Prefix-closed result chain

A `FAILED` result retains only the exact verified prefix of artifacts
produced before termination -- `quote_batch`, `portfolio_context`,
`quote_gate_batch`, `allocation_batch` -- and never a `decision_package`.
`PromotedOperationalRunResult.verify_content_identity` enforces both that
this prefix has no missing middle layer (for example, for a nonempty
preparation, retained portfolio context requires the preceding quote batch;
and a retained
`allocation_batch` requires a retained `quote_gate_batch` and
`portfolio_context`, bound to it by matching content IDs) *and* an exact
maximum stage-prefix depth per primary failure code -- not merely
gap-freedom. A depth-0 code (`START_BEFORE_WINDOW`, `START_AFTER_DEADLINE`,
`SOURCE_IDENTITY_INVALID`, `QUOTE_ACQUISITION_FAILED`,
`QUOTE_COVERAGE_INVALID`) can never retain any artifact at all, so a
self-consistently rehashed `QUOTE_ACQUISITION_FAILED` result that also
retains a complete `allocation_batch` fails replay even though nothing
about the chain itself is broken -- that combination is one the runner
itself can never produce. The full depth table:

| Failure code | Exact depth | `evaluated_at` |
|---|---|---|
| `START_BEFORE_WINDOW` / `START_AFTER_DEADLINE` | 0 (no `quote_source_id`/`portfolio_source_id` either) | `None` |
| `SOURCE_IDENTITY_INVALID` | 0 | `None` |
| `QUOTE_ACQUISITION_FAILED` / `QUOTE_COVERAGE_INVALID` | 0 | `None` |
| `PORTFOLIO_ACQUISITION_FAILED` | 1 (`quote_batch` only; 0 for a zero-candidate preparation) | `None` |
| `EVALUATION_AFTER_DEADLINE` / `QUOTE_GATE_FAILED` | 2 (`quote_batch` + `portfolio_context`) | present, `> deadline` only for `EVALUATION_AFTER_DEADLINE` |
| `ALLOCATION_FAILED` | 3 (+ `quote_gate_batch`) | present, `<= deadline` |
| `DECISION_ASSEMBLY_FAILED` / `COMPLETION_AFTER_DEADLINE` | 4 (+ `allocation_batch`, never `decision_package`) | present, `<= deadline`; `completed_at > deadline` only for `COMPLETION_AFTER_DEADLINE` |
| `CLOCK_NON_MONOTONIC` alone | 2 or 4 only ("evaluation prefix" or "completion prefix") | may be `None` at depth 2 |

`CLOCK_NON_MONOTONIC` may additionally accompany exactly one quote-
acquisition, quote-coverage, portfolio-acquisition, quote-gate, allocation,
or decision-assembly failure when that failure path's later completion-clock
read also failed. It cannot accompany a start-window, source-identity,
evaluation-deadline, or completion-deadline code; those paths do not perform
such a clock read. When combined, the primary code's exact depth still applies
unchanged. Every result also
independently re-checks its own `schema_version` on every
`verify_content_identity` call, not only at construction, so a
self-consistently rehashed result with a forged `schema_version` still
fails replay. A `COMPLETE` result requires no failure codes, `started_at`
inside the exact decision window, both source IDs equal to their pins, and
the complete chain through one exact `decision_package`, bound all the way
back to the spec's own `quote_gate_spec`/`allocation_policy`. A
zero-candidate `COMPLETE` result is the sole valid case with
`quote_batch=None` and an otherwise complete downstream chain.

## Non-goals

This module does not persist a result, send a Telegram message, register a
paper trade, call a broker, read the environment or a credential,
construct a network client, or read the real clock. It never reranks a
candidate, mutates a price, widens a policy, or resizes a quantity -- it
orchestrates the exact promoted engine outputs already implemented in
`promoted_operational_quote_gate.py`, `promoted_operational_allocation.py`,
and `promoted_operational_decision.py`. Create-once local persistence,
paper-ledger registration, and Telegram delivery around an accepted
terminal result remain the next bounded increment.
