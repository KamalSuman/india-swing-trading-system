# Point-in-time promotion gate

Status: the deterministic decision contract, typed daily-run adapter,
create-once local store, and sanitized inspection CLI are implemented. No real
artifact is upgraded by this component.

The gate separates four stages:

1. `COLLECTION_ONLY` means bytes and derived diagnostics may be archived, but
   the evidence cannot enter research.
2. `RESEARCH_ELIGIBLE` requires verified calendar, stable identity, universe,
   and raw-price coverage.
3. `BACKTEST_ELIGIBLE` additionally requires corporate actions, liquidity,
   surveillance, tick sizes, explicit nontrading state, and reconciliation.
4. `ALERT_ELIGIBLE` additionally requires completed model validation, a sealed
   risk policy, and successful shadow operations.

Every capability is represented by immutable evidence containing its exact
source snapshot IDs, knowledge cutoff, coverage interval, readiness,
completeness, actionability, and machine-readable reason codes. A promotion
decision records independent blocker sets for all three eligible stages and is
content addressed.

Missing evidence fails closed. The gate also blocks:

- `COLLECTION_ONLY` evidence;
- synthetic fixtures in a real promotion decision;
- evidence known after the requested decision cutoff;
- coverage that starts after the requested history or ends before the market
  session;
- incomplete or non-actionable evidence; and
- every source-specific reason code, namespaced by capability.

Passing the gate does not prove profitability. It only establishes that the
declared evidence satisfies the data and operational admission contract. The
existing evaluation-dataset assembler still independently verifies exact daily
calendar, universe, raw-price, stable-identity, nontrading, and tick-size
bindings before producing baseline inputs.

Evaluate one already sealed daily collection run against an explicit requested
history start:

```powershell
python -m india_swing.promotion.cli evaluate-daily-run `
  --run-id <sealed-daily-run-id> `
  --history-start 2020-01-01 `
  --tick-size-snapshot-id <optional-tick-size-snapshot-id> `
  --liquidity-snapshot-id <optional-liquidity-snapshot-id> `
  --universe-snapshot-id <optional-universe-snapshot-id>
```

The command loads the run from `INDIA_SWING_DAILY_PIPELINE_ROOT`, derives only
the collection evidence represented by that typed run, and stores the
content-addressed result under `INDIA_SWING_PROMOTION_ROOT` (default
`var/promotion`). `show --decision-id <id>` and `list` re-open and verify stored
decisions. Invalid arguments and failures expose only an error type.

The daily-run adapter currently produces diagnostics for calendar, stable
identity, universe, raw prices, liquidity, surveillance, explicit nontrading
state, and reconciliation. A separately materialized tick-size snapshot can now
be supplied with `--tick-size-snapshot-id`; it remains collection-only until
stable listing identity and source provenance are promoted. A trailing-liquidity
snapshot can similarly replace the generic liquidity placeholder through
`--liquidity-snapshot-id`. The adapter preserves all snapshot reason codes and
does not infer zero volume from absent traded-only Bhavcopy rows. Corporate
The source-backed broad-equity diagnostic can replace the generic universe
placeholder through `--universe-snapshot-id`; it preserves every exact row and
exclusion but remains unverified. Corporate actions remain explicitly missing
until their source-backed importer exists.
Model validation, risk authorization, and shadow operations remain alert-stage
requirements rather than being inferred from a collection run.

The current real archive remains `COLLECTION_ONLY`: it has only two EOD
sessions, zero promoted stable identities, unresolved surveillance state, and
no verified corporate-action coverage. The gate is expected to report these
conditions rather than issue a real trade.

The current two-session liquidity diagnostic is
`b1b9cf5ca6b9edfda61ee0e0cb0365c8852914ac9de9a85f189da2bde97637ea`.
It contains 3,574 `(validated ISIN, series)` candidates and zero candidates with
the required 120 observed sessions. Promotion decision
`b644426b912521a375fae13d30cf3d6d48eee673c4cd4c8745d2b00488a94500`
binds this diagnostic and the 16 July tick-size snapshot to the sealed 16 July
daily run; it correctly remains `COLLECTION_ONLY`.

The current broad-universe snapshot is
`f9dca3a8233f2249aee8455032c080cb670f8f1376cdd2fc747ecde3fdf05b48`.
It audits 36,062 source rows and retains 21,133 unverified equities with no
market-cap cutoff. Promotion decision
`8c15742e40bdb3c5eaa3b3c757055a43c0439877e2bbde440c1fa0a6533d0634`
binds the universe, liquidity, and tick-size diagnostics together. It remains
collection-only because stable identity, point-in-time listing state, board,
surveillance, calendar provenance, report date, and acquisition provenance are
not verified.
