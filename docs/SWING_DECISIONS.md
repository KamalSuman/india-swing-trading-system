# Deterministic swing decision package

The quote-to-decision service is the final pure decision boundary for the new
deterministic swing path. It accepts only four explicit inputs:

- one exact `SwingProposalBatch` built from cutoff-safe, point-in-time inputs;
- one immutable `FullQuoteBatch` captured for exactly the same proposal keys;
- one explicit `SwingPortfolioSnapshot` whose `as_of` does not postdate the
  quote-gate evaluation time; and
- the evaluation time, plus optional immutable quote, ranking, and sizing
  policies.

`build_swing_decision_package` then runs the complete in-memory chain:

```text
technical proposals
  -> live-quote freshness/spread/depth/circuit gate
  -> deterministic cross-sectional ranking
  -> sequential portfolio sizing
  -> one BUY or NO_TRADE daily decision
  -> human-readable research notification
```

The service never selects a latest object, reads the filesystem, obtains a Kite
credential, calls a model, sends a message, or places an order. Quote collection
and any later channel adapter remain separate capabilities.

## One-trade daily boundary

The default sizing policy allows at most one new position from a run and at most
four open positions across runs. This makes the final decision singular and
ensures its capital reservation is the same reservation shown to the user.
Additional ranked opportunities remain explicit sizing vetoes with
`MAX_NEW_POSITIONS_PER_RUN_REACHED`; they are not silently discarded.

For an INR 1,00,000 research portfolio, the new sizing defaults are:

- 0.5% / INR 500 planned risk for one trade;
- 2% / INR 2,000 maximum aggregate open risk;
- 25% maximum notional in one position;
- 80% maximum gross exposure;
- four maximum open positions and one maximum new position per run;
- 0.25% participation in historical median daily traded value;
- 20% participation in the captured best-ask quantity;
- a 1% daily realized-loss halt and 2% pilot realized-drawdown halt; and
- minimum 2.5 net reward/risk after the conservative round-trip cost.

These are loss-planning limits, not loss guarantees. Gaps, circuit limits,
liquidity withdrawal, slippage, fees, and manual delay can produce a larger
realized loss. A 5–10% return is not assumed anywhere in the contract.

## Complete decision explanation

A BUY recommendation embeds the exact sized outcome and deterministically
replays:

- comparative rank and every normalized ranking factor, weight, and
  contribution;
- last price, best ask, observed spread, quote timestamp, and applied cost;
- momentum, trend, volume, and median-traded-value evidence;
- quantity, entry-high notional, estimated round-trip cost, and planned maximum
  loss;
- entry range, stop, target, planned target-side net reward, and net
  reward/risk;
- entry and maximum-holding boundaries;
- evidence IDs; and
- explicit cancellation/re-evaluation conditions.

The ranking score is not a probability or confidence estimate. There is no 80%
confidence field and no way for an LLM or notification adapter to override a
veto or position size.

A NO_TRADE decision is also content-addressed. It preserves all available
universe, quote, and sizing veto codes rather than returning an unexplained
empty result.

## Notification outbox

`LocalSwingDecisionOutbox` is a create-once local handoff keyed by the immutable
decision ID. It accepts a fully verified `SwingDecisionPackage`, stores the
rendered notification with its SHA-256, rejects unsafe paths and links, and
refuses different bytes for an existing decision ID. Re-opening the file
strictly rejects duplicate/unknown JSON keys, malformed timestamps, changed
hashes, and identity mismatch.

Every message begins with:

```text
RESEARCH ONLY — MANUAL REVIEW REQUIRED — DO NOT AUTO-EXECUTE
```

The outbox is not a Telegram, email, broker, or GCP adapter. Production still
needs an append-only cloud publication boundary and a separate notification
channel consumer after real point-in-time data promotion is available.
