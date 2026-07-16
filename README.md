# India Swing

This repository is the deterministic core of a long-only Indian equity swing-trade
research and alert system. It is deliberately separated from the upstream
TradingAgents repository.

The current vertical slice implements:

- strict point-in-time evidence validation;
- NSE main-board eligibility without a market-cap cutoff;
- explicit Kronos, signal, and TradingAgents adapter contracts;
- deterministic ranking, sizing, cost, liquidity, and portfolio-risk gates;
- `BUY` or `NO_TRADE` output only;
- typed failed-run output for data/model/research outages, always with `NO_TRADE`;
- create-once typed pipeline audit records with run-ID, nested-integrity, and
  secret-rejection checks;
- explicit trial, model, universe, calendar, data, source, execution, and cost
  lineage in every typed pipeline result, including result-only audit writes;
- evidence-based post-trade reviews that preserve unresolved causes;
- a pinned, read-only Kite market-data adapter and immutable local snapshot store;
- a strict, collection-only importer and immutable raw archive for manually
  downloaded NSE CM MII security masters;
- a strict NSE multiple-report bundle importer that cross-reconciles UDiFF and
  delivery Bhavcopies, preserves REG1/band/series date semantics, and quarantines
  interoperability security masters;
- collection-only positive traded-date evidence plus an all-row reconciliation
  diagnostic that preserves nontraded securities and unresolved calendar state;
- a sealed manual NSE calendar-circular archive and explicit event-graph
  materializer that supports amendment chains without inventing data finality;
- a replay-verified raw, unadjusted NSE EOD price store derived from paired
  final UDiFF/full Bhavcopies;
- a sealed, positive-observation-only cross-vintage identity registry that
  detects rename candidates and identifier reuse without inventing delistings
  or assigning tradable stable IDs;
- immutable expanding purged walk-forward plans that use explicit trading
  sessions, ten-session minimum label/embargo boundaries, and nonrepeating test
  windows;
- create-once, content-addressed trial preregistrations that freeze strategy
  families, hypotheses, inputs, hashes, metrics, thresholds, cost/execution
  bindings, multiple-testing policy, and sealed-holdout identity before a run;
- append-only per-trial lifecycle chains for audited holdout unsealing/access,
  completed-negative results, failures, aborts, and later invalidations;
- content-addressed calendar/universe contracts with stable listing lineage;
- effective-dated eligibility lineage and split-session trading windows;
- stable instrument/listing/universe/data identity plus exact content
  fingerprints on model and research output;
- end-of-run component and policy revalidation, with a per-run captured risk
  policy so provider mutation cannot raise the declared rupee risk limits;
- a pipeline gate that rejects collection-only or unrelated reference artifacts;
- a synthetic demo and standard-library unit tests.

It has not yet used real account credentials or collected a live snapshot. It
also does **not** yet use real Kronos weights, an LLM, a point-in-time history of
official NSE security masters, or automatic execution. The supplied current-day
master remains a manually acquired collection-only artifact. The demo symbols
are fictional and cannot generate a real trade.

The code currently refuses to construct `POINT_IN_TIME_VERIFIED` calendar or
universe artifacts. The security-master importer preserves and validates one
official input, but it deliberately remains `COLLECTION_ONLY`; authenticated
calendar provenance, adjudicated stable identity, liquidity, corporate actions,
and multi-vintage completeness are still missing.
Only synthetic decisions can pass the end-to-end demo today. Every such decision
carries `execution_eligible=false`.

The default risk policy rejects provisional or unvalidated probability estimates.
The fictional demo opts out explicitly so the plumbing can be exercised; its
probabilities and expected return are not performance claims.

## Run locally

Use Python 3.12, the currently verified runtime, from the repository root.
Before using a newer interpreter, verify the full project test suite and the
pinned Kite SDK installation and contract tests on that version.

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
python -m india_swing.demo --output-dir var/audit
```

The optional Kite collector is documented in `docs/MARKET_DATA.md`. It requires
the pinned `kiteconnect` extra and a runtime API key plus daily access token.
The official NSE source boundary and manual/authorized ingestion rationale are
documented in `docs/REFERENCE_DATA.md`.
The selected daily-report families and their date roles are documented in
`docs/DAILY_REPORTS.md`.
The positive-date and all-row diagnostic boundary is documented in
`docs/EVIDENCE_RECONCILIATION.md`.
The event-sourced schedule boundary is documented in `docs/CALENDAR_DATA.md`.
The raw historical-price boundary is documented in `docs/HISTORICAL_PRICES.md`.
The cross-vintage identity boundary is documented in
`docs/IDENTITY_REGISTRY.md`.
The leakage-safe evaluation split boundary is documented in
`docs/EVALUATION.md`.

After manually downloading the report named **CM - MII - Security File (.gz)
(NSE Listed securities)**, import it without extracting it:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.reference_data.cli security-master import `
  --file C:\path\to\NSE_CM_security_DDMMYYYY.csv.gz
```

The command never connects to NSE. It timestamps the local first-seen and
validation events itself, rejects unknown schemas or incomplete rows, and stores
the original gzip bytes plus a deterministic normalized representation under
`var/reference_data`. Because local bytes alone do not prove their origin or
business date, the manifest records `UNVERIFIED_MANUAL_FILE`, treats the filename
date as a claim, and keeps the artifact collection-only. Import success is not
permission to generate an alert.

Import an NSE **Multiple file Download** ZIP without extracting it:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.daily_reports.cli bundle import `
  --file C:\path\to\Reports-Daily-Multiple.zip
```

The bundle importer accepts only pinned report families, preserves all outer
entries in the raw ZIP, validates every selected row, and stores ignored and
quarantined dispositions explicitly. It does not infer next-session effective
dates without a verified trading calendar, merge overlapping band files, or
turn any report into an executable signal.

After importing two or more dated security-master vintages, build sealed
continuity candidates with an explicit knowledge cutoff:

```powershell
python -m india_swing.identity_registry.cli materialize `
  --security-master-id <older-master-artifact-id> `
  --security-master-id <newer-master-artifact-id> `
  --cutoff <ISO-8601-cutoff-with-timezone>
```

A single vintage is accepted to establish the first observation baseline, but
it cannot provide cross-vintage evidence. The result remains
`COLLECTION_ONLY`, `actionable=false`, and assigns no stable instrument ID.

The demo creates one create-once audit file in `var/audit`. Running the exact same
snapshot again intentionally refuses to overwrite that file. The local record is
hash-verified and published atomically, but a filesystem administrator can still
delete or replace it. Production needs conditional Cloud Storage writes,
retention controls, and access logs.

## Safety boundary

TradingAgents will be an advisory research adapter. It cannot choose the
universe, size a position, override a veto, write an alert, or execute an order.
See `docs/TRADINGAGENTS_ADAPTER.md` and `docs/BIAS_INVARIANTS.md`.

The bias-invariant document is the release target, not a claim that every named
suite already exists. This increment covers the point-in-time cutoff, versioned
synthetic session arithmetic, split-session windows, next-session entry, a
same-session EOD finality contract, stable listing/universe lineage, main-board
eligibility, deterministic
risk gates, provider-output identity binding, explicit lineage, and local audit
integrity. An authenticated point-in-time NSE calendar, historical security-master
and lifecycle vintages, corporate-action
vintages, complete Indian charge schedule, purged walk-forward evaluation, trial
registry, immutable cloud storage, and live adapters remain required before any
real alert.

## Pilot risk defaults

For the intended Rs 1,00,000 pilot, the deterministic defaults are Rs 250 planned
risk per trade, Rs 500 aggregate open risk, at most two open positions, Rs 20,000
per position, and Rs 40,000 gross exposure. Sizing is also capped by remaining
cash and 0.25% of median daily traded value. New positions halt after Rs 750 of
daily realized loss or Rs 1,500 of cumulative pilot realized loss, preserving a
Rs 500 reserve inside the user's Rs 2,000 maximum-loss envelope for gap and
execution risk. These controls cannot guarantee a market gap will not lose more.
