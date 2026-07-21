# India Swing

This repository is the deterministic core of a long-only Indian equity swing-trade
research and alert system. It is deliberately separated from the upstream
TradingAgents repository.

The current vertical slice implements:

- strict point-in-time evidence validation;
- NSE main-board eligibility without a market-cap cutoff;
- explicit Kronos, signal, and TradingAgents adapter contracts;
- a snapshot-bound deterministic swing signal provider that derives
  explainable momentum, trend, volume, liquidity, ATR entry/stop/target levels,
  rejects raw-unadjusted or future-known evidence, keeps uncalibrated
  probabilities provisional, and accepts only snapshot-bound, preregistered,
  untouched-test calibration before marking probabilities validated;
- a promotion-replayed signal-input assembler that joins each raw NSE session
  to one exact stable listing, carries identity knowledge time through
  corporate-action adjustment, permits synthetic research, and refuses to let
  synthetic or collection-only inputs enter the alert engine;
- a content-addressed full-universe input batch that requires exactly one
  verified assembly for every actionable listing, preserves explicit vetoes for
  every other listing, and never applies a market-cap filter or silently accepts
  a partial scan;
- deterministic ranking, sizing, delivery/intraday cost, liquidity, and portfolio-risk gates;
- a complete quote-to-decision package that emits at most one content-addressed
  BUY/NO_TRADE research decision with full factor, quote, sizing, target/stop,
  cancellation, and veto rationale, plus a create-once local notification outbox;
- a paper-only operational runner with deterministic 500-symbol Kite chunking,
  injected portfolio/clock boundaries, sanitized fail-closed results, one
  terminal manifest per immutable run spec, and automatic paper registration;
- `BUY` or `NO_TRADE` output only;
- typed failed-run output for data/model/research outages, always with `NO_TRADE`;
- create-once typed pipeline audit records with run-ID, nested-integrity, and
  secret-rejection checks;
- explicit trial, model, universe, calendar, data, source, execution, and cost
  lineage in every typed pipeline result, including result-only audit writes;
- purged-fold-bound evaluation that generates conservative fills, itemized
  cash-equity costs, mark-to-market equity, fixed metrics, threshold outcomes,
  base/stressed benchmark comparisons, and create-once full-result evidence;
- preregistration-bound deterministic close-momentum strategy and liquid
  equal-weight benchmark intent generators, with explicit point-in-time
  eligibility, as-of evidence IDs, a decision or veto for every candidate,
  create-once per-role batches, per-fold dispersion, a Holm familywise gate,
  persisted research promotion, create-once deterministic run/report manifests,
  and a report publication/inspection CLI;
- a content-addressed evaluation-dataset assembler and create-once local store
  that require gap-free versioned sessions, one universe snapshot per session,
  adjudicated stable listing/ISIN identity, exact missing-row evidence, and
  effective-dated tick sizes before producing baseline inputs;
- a content-bound promotion gate that independently reports research, backtest,
  and alert blockers for missing, partial, synthetic, future-known, or
  collection-only evidence instead of laundering ingestion into readiness,
  plus create-once storage and a sanitized CLI for daily-run diagnostics;
- a point-in-time corporate-action ledger contract that preserves amendments
  and cancellations, supports explicit split/bonus ratios and cash dividends,
  and refuses unsafe automatic factors for complex actions;
- a content-addressed corporate-action adjustment engine that applies only safe
  split/bonus price-volume factors, requires an exact stable-identity binding
  for every raw bar, and adapts the result into signal-engine evidence without
  rewriting raw NSE history;
- a replay-verified collection-only tick-size materializer sourced from the NSE
  security master's paise-denominated `BidIntrvl`, with exact Decimal conversion,
  reserved-field change detection, create-once storage, CLI, and promotion
  evidence;
- a replay-verified collection universe that audits every security-master row,
  retains all 21,133 source-classified equities without a market-cap cutoff,
  records exact exclusions, and refuses to assign provisional stable identities
  or tradability;
- a replay-verified trailing-liquidity materializer that computes exact median
  traded value, volume, and available delivery percentage from sealed EOD
  sessions, while refusing to treat traded-row-only coverage as zero-volume or
  point-in-time universe evidence;
- evidence-based post-trade reviews that preserve unresolved causes;
- integrity-bound `RESEARCH_ONLY` shadow candidate/`NO_TRADE` messages with a
  complete thesis and create-once local notification outbox;
- an exact-ID, collection-only shadow scanner that ranks no observation until
  explicit history, liquidity, delivery, universe, price, and tick-size gates
  pass, while never producing trade levels or execution authority;
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
  or assigning tradable stable IDs, plus a create-once complete adjudication
  queue that derives the official evidence required for every candidate;
- a content-addressed, collection-only NSE identity/lifecycle evidence archive
  for exact Listing circular PDFs and corporate-action CSVs, with declarations
  bound to exact candidate/requirement pairs and a coverage report that cannot
  adjudicate cases or assign stable IDs;
- explicit human review bundles and a partial stable-identity materializer that
  rejects duplicate or mismatched decisions, preserves one listing ID across a
  same-series symbol rename, records only observed effective dates, and remains
  collection-only even after assignment;
- an explicit-predecessor daily collection runner that imports one session,
  derives prices and reconciliation, rebuilds the identity registry/queue, and
  persists one content-addressed completeness report without any implicit
  latest-file selection;
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
also does **not** yet use real Kronos weights, an LLM, a sufficiently long and
verified point-in-time history of official NSE security masters, or automatic
execution. The two currently archived consecutive masters remain manually
acquired collection-only artifacts. The demo symbols are fictional and cannot
generate a real trade.

The code currently refuses to construct `POINT_IN_TIME_VERIFIED` calendar or
universe artifacts. The security-master importer preserves and validates each
official input, but they deliberately remain `COLLECTION_ONLY`; authenticated
calendar provenance, adjudicated stable identity, liquidity, corporate actions,
and multi-vintage completeness are still missing.
The identity-evidence archive can collect official documents, but no reviewed
decision has yet been supplied for the real queue. The reviewed-decision and
partial stable-ID mechanism is implemented, but its real snapshot assigns zero
identities until those decisions exist.
Only synthetic decisions can pass the end-to-end demo today. Every such decision
carries `execution_eligible=false`.

The evaluation-dataset assembler is implemented, but it does not upgrade any
manual NSE file. It normalizes the existing raw EOD artifact without changing
its `COLLECTION_ONLY` status, then refuses it. A real dataset can be assembled
only after the upstream calendar, daily universes, identities, price finality,
explicit nontrading rows, and tick-size evidence are independently promoted to
point-in-time verified artifacts.

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
The promotion stages and corporate-action boundary are documented in
`docs/PROMOTION.md` and `docs/CORPORATE_ACTIONS.md`.
The security-master tick-size boundary is documented in `docs/TICK_SIZES.md`.
The trailing-liquidity collection boundary is documented in `docs/LIQUIDITY.md`.
The broad collection-universe boundary is documented in `docs/UNIVERSE.md`.
The cross-vintage identity boundary is documented in
`docs/IDENTITY_REGISTRY.md`.
The explicit daily orchestration and predecessor boundary is documented in
`docs/DAILY_PIPELINE.md`.
The leakage-safe evaluation split boundary is documented in
`docs/EVALUATION.md`.
The effective-dated delivery-cost and conservative fill policy is documented in
`docs/COSTS_AND_EXECUTION.md`.
The non-executable paper-notification boundary is documented in
`docs/SHADOW_ALERTS.md`.
The explicit-input observation scanner is documented in
`docs/SHADOW_SCANNER.md`.
The deterministic signal and trade-level engine is documented in
`docs/DETERMINISTIC_SWING_ENGINE.md`.
The final quote-to-decision and notification package is documented in
`docs/SWING_DECISIONS.md`.
The operational quote, publication, and paper-registration boundary is
documented in `docs/SWING_OPERATIONS.md`.

Persist or inspect a family evaluation report after its registrations, runs,
comparisons, and family aggregate have already been sealed:

```powershell
python -m india_swing.evaluation.cli report publish `
  --strategy-family-id <registered-family-id>

python -m india_swing.evaluation.cli report show `
  --aggregate-id <family-aggregate-id>

python -m india_swing.evaluation.cli report list
```

The CLI reads `INDIA_SWING_TRIAL_REGISTRY_ROOT` and
`INDIA_SWING_EVALUATION_ROOT`; failures emit only a sanitized error type.

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

After the raw inputs and calendar have been collected, run one complete daily
derivation with an explicit predecessor:

```powershell
python -m india_swing.daily_pipeline.cli run `
  --session <YYYY-MM-DD> `
  --cutoff <ISO-8601-cutoff-with-timezone> `
  --calendar-id <sealed-calendar-materialization-id> `
  --security-master-file C:\path\to\NSE_CM_security_DDMMYYYY.csv.gz `
  --daily-bundle-file C:\path\to\Reports-Daily-Multiple.zip `
  --previous-run-id <immediately-preceding-session-run-id>
```

Omit `--previous-run-id` only for an explicit bootstrap. Such a run records
`NO_PREVIOUS_DAILY_RUN` and remains a collection diagnostic.

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
and lifecycle vintages, an official corporate-action importer and verified
vintages, historical Indian charge schedules, engine-generated evaluation metrics, trial
registry, immutable cloud storage, and live adapters remain required before any
real alert.

## Pilot risk defaults

The operational service can now start from one explicitly named, create-once
proposal manifest. It resolves the exact universe batch, calendar snapshot, and
signal policy by content ID, deterministically replays the proposal batch, and
rejects any lineage or coverage difference before quotes are requested. See
`docs/SWING_OPERATIONS.md` for the stored-ID service and inspection commands.
The three parent objects are also stored in a strict type-directed archive, so
the same replay works after a process or Cloud Run job restart without an
in-memory fixture or a `latest` lookup.

`india-swing-proposal-prepare` now builds that terminal proposal manifest from
the three exact stored parent IDs. Its preparation record binds every promoted
subject assembly and promotion-decision ID, preserving complete small-cap and
veto coverage rather than silently shrinking the universe. Collection-only
inputs remain blocked. See `docs/PROPOSAL_PREPARATION.md`.

The dedicated `india-swing-operational-job` entrypoint now binds that replay to
one canonical job spec, an evidence-backed paper-only portfolio snapshot, the
read-only Kite full-quote adapter, idempotent notification/paper-ledger writes,
and a terminal operational record. It requires a pre-restored writable state
root, then durably publishes its notification, optional paper registration,
terminal record, and terminal-last exact-generation manifest to a private GCS
bucket. `india-swing-operational-restore` rebuilds those local outputs only from
an externally retained manifest object name, generation, and SHA-256—never a
bucket listing or `latest` lookup. Every terminal result is then delivered to a
configured private Telegram chat through the free Bot API, with protected
content, a stable operational record ID, sanitized failures, and a create-once
local receipt. No broker execution authority is attached to that delivery.

The paper lifecycle also has a restart-safe outcome job. It resolves entries
and exits from exact, sealed EOD artifacts with conservative stop-first
ordering, appends only the missing paper-ledger prefix, records estimated net
P&L and evidence-limited win/loss attribution, publishes a terminal-last GCS
manifest, supports exact-generation restoration, and sends Telegram only after
durable publication. It never lists for a latest artifact, fabricates a news
cause, rewrites portfolio evidence, retrains a model, or places an order. See
`docs/PAPER_OUTCOMES.md`.

An exact-ID daily paper-portfolio batch now advances all explicitly named
active outcomes together, refuses to drop an active registration, counts each
closure once, and seals cumulative estimated P&L, drawdown, expectancy, costs,
open risk, and loss-halt reasons into a create-once state. Its aggregate GCS
manifest is written terminal-last and restored only by exact generation and
hash. This remains paper-only accounting and has no broker-order capability.
Its preparation command derives the complete active set from the fail-closed
local ledger, seals jobs only from an explicit evidence inventory, and refuses
missing registrations, accounting-chain resets, and non-leaf predecessors.
An exact-ID bridge now creates that inventory directly from a sealed daily run
and `DailyDerivedEvidence`; every trade retains a decision-time tick snapshot,
so a later snapshot can never leak into entry or outcome replay.

`india-swing-daily-workflow-job` composes that bridge, outcome reconciliation,
aggregate portfolio accounting, immutable GCS publication, and Telegram into
one bounded-retry EOD workflow. Its append-only attempt log distinguishes
started, completed, rejected, and failed attempts; a durable terminal suppresses
domain reruns, and an empty active set produces one explicit paper-only
heartbeat instead of a fabricated portfolio genesis. See
`docs/DAILY_WORKFLOW.md`.

For the intended Rs 1,00,000 research pilot, the current quote-to-decision
defaults are Rs 500 planned risk per trade, Rs 2,000 aggregate open risk, at most
four open positions across runs, one new position per run, Rs 25,000 per
position, and Rs 80,000 gross exposure. Sizing is also capped by remaining cash,
0.25% of median daily traded value, and 20% of captured best-ask quantity. New
positions halt after Rs 1,000 of daily realized loss or Rs 2,000 of cumulative
pilot realized loss. These are planning limits, not guarantees: gaps, circuit
limits, liquidity withdrawal, costs, or manual delay can lose more. The engine
does not assume or promise a 5–10% return.
