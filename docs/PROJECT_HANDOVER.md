# Project handover: India Swing

Snapshot date: 2026-07-16 (Asia/Kolkata)

## One-line status

The repository has a tested, fail-closed foundation for point-in-time Indian
equity research, plus sealed NSE reference, daily-report, calendar, and raw EOD
price archives. It cannot yet issue a real trade alert: all real-file artifacts
are deliberately `COLLECTION_ONLY` and `actionable=false`.

## Repository and checkpoint

- Local repository: `C:\project\india-swing-trading-system`
- Private remote: `https://github.com/KamalSuman/india-swing-trading-system.git`
- Working branch: `agent/event-sourced-calendar`
- Implementation checkpoint: `6aee3a8` (`Add point-in-time calendar and raw price archives`)
- Remote `main`: `d5d4fda`
- The working branch has no upstream and is not on GitHub at this snapshot.
- Verified runtime: Python 3.12
- Verification: 262 unit tests passed, 3 skipped; `compileall` passed; `git diff --check` passed.

The handover document may be committed after the implementation checkpoint, so
use `git log -2 --oneline` to see the exact local tip.

## Product intent and risk envelope

The intended product is a long-only NSE cash-equity swing research and
notification system. It should scan a broad main-board universe, including
small caps, explain every candidate and veto, and let the user execute manually
through Zerodha. Automatic order execution is out of scope for the first pilot.

The user described approximately Rs 1,00,000 of eventual capital, a desired
5-10% return, and a maximum tolerable loss of Rs 2,000. Returns cannot be
promised and ten calendar days cannot establish a strategy edge. Current
deterministic pilot defaults are deliberately smaller: Rs 250 planned risk per
trade, Rs 500 aggregate open risk, two positions, Rs 20,000 per position,
Rs 40,000 gross exposure, and stop-new-entry thresholds described in the
README. Stops cannot guarantee a Rs 2,000 maximum because gaps and execution
slippage can exceed planned risk.

## Non-negotiable research invariants

1. Event time is never substituted for knowledge time. A report dated D is not
   usable before the system can prove it was available and validated.
2. Every decision is reproduced as of one UTC-normalized cutoff. Future files,
   amendments, identities, prices, corporate actions, news, and outcomes are
   excluded.
3. Universe membership starts from a point-in-time security master, never from
   stocks that happened to trade or survive into the latest Bhavcopy.
4. Absence from a Bhavcopy is not evidence of delisting, ineligibility,
   suspension, or a holiday.
5. Raw prices are immutable and unadjusted. Corporate actions must create a
   separate cutoff-specific view and must never rewrite raw bars.
6. A current Kite instrument dump or current company list cannot reconstruct
   historical membership. Kite can later provide live/pinned market snapshots,
   but it does not solve the historical point-in-time dataset.
7. Manual downloads and human transcriptions remain `COLLECTION_ONLY`; they do
   not silently become verified because parsing succeeded.
8. LLMs, TradingAgents, Kronos, and news models are advisory rankers/explainers.
   They cannot choose membership, bypass data/risk vetoes, size positions,
   manufacture confidence, send alerts, or place orders.
9. No alert is allowed unless calendar, universe, data, model, cost, liquidity,
   portfolio, and audit gates all pass. Any outage or identity mismatch is
   `NO_TRADE`.
10. Every losing and winning alert must get the same evidence-based post-trade
    review. The review may classify a cause as unresolved; it must not rewrite
    history or retrain on hindsight labels automatically.

## Implemented architecture

The code is independent of the upstream TradingAgents repository. The main
packages under `src/india_swing` are:

- Core contracts and pipeline: typed inputs, lineage, deterministic ranking,
  sizing, costs, liquidity, risk vetoes, `BUY`/`NO_TRADE`, and create-once audits.
- `market_data`: pinned, read-only Kite snapshot adapter and immutable local
  store. No real credentials have been used.
- `reference_data`: strict archive for the NSE CM MII security master gzip.
- `daily_reports`: strict archive and cross-validation for the NSE Multiple File
  Download ZIP, including paired final UDiFF and delivery Bhavcopies.
- `reconciliation`: positive traded-date evidence and all-row same-vintage
  security-master reconciliation, without dropping nontraded securities.
- `calendar_data`: exact PDF/declaration archive and deterministic explicit
  event-graph calendar materializer. It rejects cycles, unknown predecessors,
  competing branches, uncovered dates, and implicit latest-wins logic.
- `historical_prices`: replay-verified raw NSE EOD session bars derived from the
  paired final UDiFF/full Bhavcopies, with row-level lineage.
- Synthetic demo adapters for Kronos, signals, and TradingAgents. These prove
  contracts only; they are not real models or performance evidence.

See `README.md`, `docs/BIAS_INVARIANTS.md`, `docs/CALENDAR_DATA.md`,
`docs/HISTORICAL_PRICES.md`, and `docs/TRADINGAGENTS_ADAPTER.md` before changing
promotion or decision logic.

## Real artifacts already validated

All paths below are local sealed stores under `var/`. Reads re-open and reparse
the original bytes rather than trusting a cached Python object.

### NSE security master

- Source file: `NSE_CM_security_15072026.csv.gz`
- Artifact ID: `726f6c1ff4588cee1c072d6769035ed9035c5310f98321409df5ab6e3cd1efc4`
- Manifest ID: `466d74e5c1062f680518c3651726ee2bb7a56d885db129b83eca851c3827b4c0`

### NSE daily bundle

- Source file: `Reports-Daily-Multiple.zip`
- Artifact ID: `44e2079041e3b05a43703bc63e030d4ebce44b2cb05d4209177adc7431844b6b`
- Validated at: `2026-07-15T14:37:15.502701+00:00`
- Selected rows: 24,609

### Positive traded-date evidence

- Artifact ID: `92cdc918f207226eb0137bd59f83cc1ce9cb72b71b16de060fa7fd64033e05c1`
- Observed dates: 2026-07-14 and 2026-07-15

### Reconciliation diagnostic

- Snapshot ID: `f872907c3f4951c0e62b3473676cf8f0804f0ec4d912c7f05a7ff4cfdfcc199d`
- Retained master rows: 21,133
- Broad EQ scope: 3,510, including small caps
- SM watch-only scope: 772
- Other explicitly unsupported series: 16,851
- Same-session retained rows with trade evidence: 2,834
- Daily-report orphan keys retained as orphans: 2,686
- Supported rows unresolved without a verified calendar: 4,282
- Actionable rows: zero

### Raw EOD historical-price session

- Market session: 2026-07-15
- Artifact ID: `ebc8a722e47fb9bc52b0c118550b85daf8f714f224d3feadb7a8f64a9e194c7f`
- Manifest ID: `5cbcf5e581533188edf478c52d8d1614ab9ad6bb461c3a080b215d38c343b277`
- Bars/UDiFF rows: 3,409
- Delivery rows: 3,256
- Price basis: `RAW_UNADJUSTED`
- Coverage: `TRADED_ROWS_ONLY`
- Readiness: `COLLECTION_ONLY`; actionable: false

## Essential local commands

Run from `C:\project\india-swing-trading-system`:

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
python -m india_swing.demo --output-dir var/audit
```

Import sealed sources:

```powershell
python -m india_swing.reference_data.cli security-master import `
  --file C:\path\to\NSE_CM_security_DDMMYYYY.csv.gz

python -m india_swing.daily_reports.cli bundle import `
  --file C:\path\to\Reports-Daily-Multiple.zip
```

Create evidence and reconciliation:

```powershell
python -m india_swing.reconciliation.cli observed-dates `
  --daily-bundle-id <bundle-id> `
  --cutoff <ISO-8601-cutoff>

python -m india_swing.reconciliation.cli reconcile `
  --security-master-id <master-id> `
  --daily-bundle-id <bundle-id> `
  --market-session 2026-07-15 `
  --cutoff <ISO-8601-cutoff> `
  --calendar-id <sealed-calendar-materialization-id>
```

Import and materialize a calendar only after preparing the strict companion
declaration described in `docs/CALENDAR_DATA.md`:

```powershell
python -m india_swing.calendar_data.cli source-import `
  --source-pdf C:\path\to\official-circular.pdf `
  --declaration C:\path\to\official-circular.events.json

python -m india_swing.calendar_data.cli materialize `
  --source-id <base-source-id> `
  --source-id <holiday-or-amendment-source-id> `
  --coverage-start <YYYY-MM-DD> `
  --coverage-end <YYYY-MM-DD> `
  --cutoff <ISO-8601-cutoff> `
  --observed-daily-bundle-id <bundle-id>
```

Materialize one raw EOD session:

```powershell
python -m india_swing.historical_prices.cli materialize `
  --daily-bundle-id <bundle-id> `
  --market-session <YYYY-MM-DD> `
  --cutoff <ISO-8601-cutoff>
```

## What is not implemented

- Authenticated/licensed, automatically acquired point-in-time NSE calendar.
- A complete set of real calendar circulars and declarations for the target
  coverage period.
- Historical daily security-master vintages, delistings, renames, mergers, and
  a stable cross-vintage instrument/company identity registry.
- Official corporate-action ingestion and cutoff-specific adjusted views.
- Multi-year survivorship-safe price history.
- A production liquidity/eligibility universe promoted to
  `POINT_IN_TIME_VERIFIED`.
- A fitted strategy, Kronos weights, calibrated probabilities, news feed,
  purged walk-forward backtest, shadow/paper alerts, or performance report.
- Cloud Storage immutability, Cloud Run scheduling, Secret Manager wiring,
  monitoring, notifications, or live Zerodha execution.

## Recommended next milestones, in order

1. Review `6aee3a8`, push this branch, and open a PR. Do not merge merely because
   tests pass; review the promotion boundaries and raw-data replay cost.
2. Collect the official NSE base trading schedule, annual holiday circular, and
   every amendment, closure, or special-session circular needed for current
   date plus at least ten future sessions. Create and independently verify the
   strict JSON declarations, then materialize and seal the bounded calendar.
3. Re-run reconciliation with `--calendar-id`. Calendar schedule resolution is
   diagnostic until acquisition provenance and independent provider finality
   are verified.
4. Establish a recurring authorized collection job for the daily security
   master and Multiple File Download bundle. Materialize each raw EOD session.
5. Build cross-vintage identity and listing-status history. This is the key
   survivorship-bias boundary before backtesting.
6. Add an official corporate-action source using a real archived fixture;
   design its schema from the source rather than guessing it.
7. Implement a deterministic non-LLM baseline strategy and evaluate it with
   purged walk-forward splits, realistic Indian costs, slippage, liquidity,
   delistings, and registered trials. Compare against simple benchmarks.
8. Add Kronos and/or TradingAgents only if they improve out-of-sample ranking.
   Add point-in-time news with publication/revision timestamps. Calibrate any
   confidence score; an uncalibrated 80% label is not an 80% probability.
9. Run shadow alerts and paper trading first. Produce the full trade thesis and
   identical post-trade attribution for wins and losses. Only then consider a
   small, explicitly capped capital pilot.
10. Move the deterministic service to GCP using Cloud Run/Jobs, Cloud Scheduler,
    immutable/versioned Cloud Storage, Secret Manager, and alert monitoring.

## Inputs required from the owner

- The official NSE calendar PDFs for the intended period: base schedule,
  holiday calendar, amendments/closures, and special sessions. The declarations
  need human verification because they transcribe executable dates and windows.
- A decision on an authorized recurring NSE data source/license. Manual portal
  downloads are acceptable for collection experiments but not unattended
  production.
- A real official corporate-action export or an authorized source sample before
  that importer is designed.
- Zerodha/Kite credentials only when live snapshot collection is reached. Put
  them in environment variables or GCP Secret Manager; never paste secrets into
  chat, source code, fixtures, commits, or handover files.

## Honest progress assessment

Approximately 50% of a research-and-notification MVP foundation is implemented,
but only about 25-30% of the work required for a defensible real-capital pilot.
The system is 0% live-trade-ready because it correctly refuses all real alerts.
The largest remaining effort is trustworthy historical data and evaluation,
not connecting an LLM or formatting a notification.

