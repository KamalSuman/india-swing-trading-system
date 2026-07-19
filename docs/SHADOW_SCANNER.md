# Collection-only shadow scanner

The collection shadow scanner is an observation tool for the period while
official point-in-time history is still accumulating. It does not weaken or
replace the production pipeline, promotion gate, backtest admission gate, or
shadow-alert authority boundary.

## Inputs

One scan requires an exact `DailyDerivedEvidence` ID. That artifact binds:

- an ordered tuple of sealed raw EOD price artifact IDs;
- one exact broad collection-universe snapshot;
- one exact trailing-liquidity snapshot;
- one exact tick-size snapshot;
- the target session, decision cutoff, and minimum-history policy.

The scanner loads those IDs directly. It never lists a directory to select the
latest file. Before scoring, it independently re-verifies every content identity,
the source order, cutoff, target session, snapshot bindings, liquidity source
bindings, collection-only readiness, and non-actionable state.

## Default policy

- 120 supplied historical sessions;
- 20-session raw close momentum;
- minimum INR 1 crore median daily traded value;
- minimum 20% median delivery;
- `EQ` series only;
- at most 20 ranked observations.

Every lookback session must contain an identity-matched traded bar. A missing
bar is excluded rather than interpreted as a zero return, suspension, delisting,
or holiday. Current universe, price, liquidity, and tick observations must agree
on ISIN/series and the current session-scoped financial instrument ID.

The score is deliberately simple: raw lookback close return, with median traded
value and symbol used only as deterministic tie-breakers. It is an unvalidated
baseline observation, not a fitted strategy or confidence estimate.

## Output boundary

The result is always `RESEARCH_ONLY`, `actionable=false`, and carries blockers
for:

- collection-only acquisition;
- unverified stable identity;
- raw, corporate-action-unadjusted prices;
- insufficient history when applicable.

A ranked observation has no quantity, entry range, stop, target, or permission
to notify. Those fields can only be created later by separately reviewed signal,
risk, and shadow-alert stages.

## Optional local publication

Passing `--publish` writes the exact result to a create-once,
content-addressed local store. The default path is
`var/shadow_scans/results/<result_id>.json`; set
`INDIA_SWING_SHADOW_SCAN_ROOT` to choose a different root. Re-publishing the
same result is idempotent, while malformed, altered, linked, or differently
encoded content is rejected on read. Publication does not make the observation
actionable and does not send a notification.

## Run the current sealed archive

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.shadow_scanner.cli `
  --derived-evidence-id 8e88351a1347fdb3c32bfdd0d4990891efe80eac2366804462fff264669747a0 `
  --publish
```

The current 15–16 July archive has only two supplied sessions. Its verified
result is therefore `NO_CANDIDATE`, with all 21,133 broad-scope observations
classified as `INSUFFICIENT_GLOBAL_HISTORY`. This is the expected safe result.

Environment roots are inherited from the existing daily-pipeline, historical-
price, daily-report, reference-data, universe, liquidity, and tick-size config
contracts. Errors print only their type and never echo argument contents.
