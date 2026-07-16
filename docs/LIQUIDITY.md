# Trailing liquidity snapshots

Status: the collection-only materializer, exact metric contract, replay-verified
local store, sanitized CLI, and promotion adapter are implemented. No liquidity
snapshot is currently eligible for backtesting or alerts.

## What is measured

For every `(validated ISIN, series)` observed in the supplied sealed raw EOD
sessions, the materializer records:

- exact median daily traded value;
- exact median daily volume;
- exact median delivery percentage when delivery evidence exists;
- the exact observed session and source-bar IDs; and
- whether the candidate has the configured minimum observed history, currently
  120 sessions.

All medians use `Decimal`, including the midpoint of an even number of values.
Only source sessions whose knowledge time is at or before the decision cutoff
may enter the snapshot. Source sessions must be unique and chronological.

## Deliberate fail-closed boundary

The NSE EOD artifacts currently have `TRADED_ROWS_ONLY` coverage. A missing row
therefore does not prove zero volume, suspension, delisting, or a nontrading
session. The collection snapshot consequently does not claim calendar
continuity or complete universe coverage. It also uses the validated ISIN and
series only as a diagnostic candidate key; that pair is not a substitute for an
adjudicated stable listing identity.

These limitations are retained as machine-readable reasons and propagated into
the promotion decision. Supplying a liquidity snapshot replaces the generic
missing-liquidity diagnostic; it cannot upgrade the evidence by itself.

## CLI

Materialize an explicit ordered chain of historical-price artifacts:

```powershell
python -m india_swing.liquidity.cli materialize `
  --historical-price-id <oldest-artifact-id> `
  --historical-price-id <newest-artifact-id> `
  --cutoff <ISO-8601-cutoff> `
  --minimum-history-sessions 120
```

Inspect stored snapshots with `show --snapshot-id <id>` or `list`. The store
re-opens the source historical-price artifacts, verifies their hashes, and
re-materializes the snapshot before returning it. Set
`INDIA_SWING_LIQUIDITY_ROOT` to override the default `var/liquidity` root.

## Current real diagnostic

The 15–16 July 2026 collection snapshot is
`b1b9cf5ca6b9edfda61ee0e0cb0365c8852914ac9de9a85f189da2bde97637ea`.
It contains 3,574 candidate groups from two source sessions, with zero meeting
the 120-session minimum. Its exact blockers are:

- unverified calendar continuity;
- insufficient history;
- traded-row-only source coverage;
- unavailable stable identity; and
- unverified manual acquisition.

This is a useful integrity and pipeline diagnostic, not evidence of a tradable
liquidity universe.
