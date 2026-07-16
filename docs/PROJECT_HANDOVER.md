# Project handover: India Swing

Snapshot date: 2026-07-16 (Asia/Kolkata)

## One-line status

The repository has a tested, fail-closed foundation for point-in-time Indian
equity research, plus sealed NSE reference, daily-report, calendar, raw EOD
price, and cross-vintage identity-candidate archives. It cannot yet issue a real
trade alert: all real-file artifacts are deliberately `COLLECTION_ONLY` and
`actionable=false`.

## Repository and checkpoint

- Local repository: `C:\project\india-swing-trading-system`
- Private remote: `https://github.com/KamalSuman/india-swing-trading-system.git`
- Working branch: `agent/cross-vintage-identity`
- Implementation checkpoint: current branch tip (`Compare stressed strategy and benchmark evidence`)
- Remote `main`: `a21333b`
- The working branch has no upstream and is not on GitHub at this snapshot.
- Verified runtime: Python 3.12
- Last full verification: 287 unit tests run: 284 passed and 3 skipped. The
  current cost/execution/evaluation integration has 76 focused tests passing;
  touched-source `compileall` and `git diff --check` passed.

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
- `identity_registry`: replay-verified positive observations, ISIN-level
  continuity candidates, unambiguous adjacent-vintage listing transitions, and
  explicit identifier/series conflicts. It assigns no stable tradable IDs.
- `evaluation`: immutable expanding purged walk-forward folds over a versioned
  trading-session tuple, plus create-once content-addressed trial
  preregistrations with same-family parent lineage and append-only lifecycle
  chains for holdout access and outcomes. Its engine generates fills, itemized
  costs, mark-to-market equity, fixed metrics, and threshold results while
  rejecting training/validation signals and collection-only data. Full results
  are create-once artifacts. Strategy and benchmark now run through identical
  base/stressed paths, and their persisted comparison is required before
  lifecycle completion.
- `execution`: a content-bound Zerodha/NSE delivery plus fully-netted intraday
  tariff effective from 2026-03-01 and a pessimistic daily-bar simulator for
  next-session entries, gaps, stops/targets, tick rounding, participation
  limits, and circuit locks. It is wired to engine-generated
  synthetic/verified-data trial performance.
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

### NSE CM calendar sources and materialization

- Base schedule source `NSE-CMTR-73927`: `619daa17de902975f4d10247d2277819969573d40e21d03212d1b29c92c6dfb3`
- Annual holidays source `NSE-CMTR-71775`: `4045763fb4d759aafd0027392d8daf50c51dd2f7834bab2fbcda555b97775bc6`
- January 15 amendment `NSE-CMTR-72260`: `4deedb475933d5d76cfd7a5a20b33989fb3e612bb9ab93712a76ba4a87905619`
- Materialization ID: `e9c240e72447a3b0ad061dd2fe79cb617e7e36120f9e04f9757cc5fc5e87463a`
- Calendar snapshot ID: `1457b00b776c1ffe8695c602c6216ace685ba0c31f5bcce67507f2447955771f`
- Coverage: 2026-01-01 through 2026-07-31; 212 days and 142 sessions
- Evidence cross-check: one sealed daily bundle covering positive trade dates
- Readiness: `COLLECTION_ONLY`; actionable: false

### Calendar-backed reconciliation diagnostic

- Snapshot ID: `df53480a2c5c30a9a1a1e28842e00fa77e34fefdd2fdd98997148232fb8ebbd9`
- Retained master rows: 21,133
- Broad EQ scope: 3,510, including small caps
- SM watch-only scope: 772
- Other explicitly unsupported series: 16,851
- Same-session retained rows with trade evidence: 2,834
- Daily-report orphan keys retained as orphans: 2,686
- Supported rows unresolved after calendar resolution: 1,852
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

### Cross-vintage identity baseline

- Source vintages: one, claimed 2026-07-15
- Registry ID: `cfb7a107d192a539f429c535cc220d677ea770a989530ec00ed9414280c3b27b`
- Manifest ID: `1bb2e0f8b3711b3c7205c01b1659fa73a857b30951b6ead44076dfb2dd3fd697`
- Positive observations: 21,133
- ISIN/unvalidated identifier candidates: 4,498
- Quarantined same-ISIN/same-series ambiguities: 18
- Cross-vintage transitions: zero, because only one dated source exists
- Stable identities assigned: zero
- Readiness: `COLLECTION_ONLY`; actionable: false

The 18 conflicts are mostly simultaneous old/new ticker rows, often with one
`DelFlg=Y` row. Their meaning has not been inferred from the flag alone; they
remain quarantined pending official lifecycle evidence.

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

Materialize identity candidates from one baseline or multiple dated masters:

```powershell
python -m india_swing.identity_registry.cli materialize `
  --security-master-id <older-master-artifact-id> `
  --security-master-id <newer-master-artifact-id> `
  --cutoff <ISO-8601-cutoff>
```

## What is not implemented

- Authenticated/licensed, automatically acquired point-in-time NSE calendar.
- Calendar changes after 2026-07-31, including the August closing-auction
  transition and later special-session circulars.
- Multiple consecutive historical security-master vintages and official
  adjudication of delistings, suspensions, renames, mergers, demergers, and
  stable instrument/listing IDs. The candidate registry is implemented, but a
  single real vintage cannot establish cross-date continuity.
- Official corporate-action ingestion and cutoff-specific adjusted views.
- Multi-year survivorship-safe price history.
- A production liquidity/eligibility universe promoted to
  `POINT_IN_TIME_VERIFIED`.
- Historical cost schedules before 2026-03-01, point-in-time tick-size views,
  partial same-day allocation evidence, and shadow contract-note reconciliation.
  The current Zerodha resident-retail NSE cash schedule does not cover other
  account/product/exchange tariffs or dealer/auto-square-off surcharges.
- A fitted strategy, Kronos weights, calibrated probabilities, news feed,
  real-data purged walk-forward backtest, shadow/paper alerts, or performance report.
- A real deterministic baseline strategy and benchmark intent generator,
  multiple-testing aggregation, and a report over actual historical folds.
- Cloud Storage immutability, Cloud Run scheduling, Secret Manager wiring,
  monitoring, notifications, or live Zerodha execution.

## Recommended next milestones, in order

1. Review the latest local branch, push it, and open a PR. Do not merge merely because
   tests pass; review the promotion boundaries and raw-data replay cost.
2. Independently review the three local calendar declarations against PDF pages
   45-47, pages 1-2, and page 1 respectively. Calendar schedule resolution stays
   diagnostic until acquisition provenance and provider finality are verified.
3. Collect and model the August 2026 closing-auction transition before extending
   calendar coverage beyond July 31.
4. Establish a recurring authorized collection job for the daily security
   master and Multiple File Download bundle. Materialize each raw EOD session.
5. Feed consecutive masters into the implemented identity registry, review its
   candidate transitions/conflicts, and add official listing-status evidence to
   adjudicate stable effective-dated IDs. This remains the key survivorship-bias
   boundary before backtesting.
6. Implement the deterministic non-LLM baseline and benchmark intent generators,
   then aggregate their generated comparisons across registered folds. The
   one-session real archive remains ineligible.
7. Add an official corporate-action source using a real archived fixture;
   design its schema from the source rather than guessing it.
8. Implement a deterministic non-LLM baseline strategy and evaluate it with
   purged walk-forward splits, realistic Indian costs, slippage, liquidity,
   delistings, and registered trials. Compare against simple benchmarks.
9. Add Kronos and/or TradingAgents only if they improve out-of-sample ranking.
   Add point-in-time news with publication/revision timestamps. Calibrate any
   confidence score; an uncalibrated 80% label is not an 80% probability.
10. Run shadow alerts and paper trading first. Produce the full trade thesis and
   identical post-trade attribution for wins and losses. Only then consider a
   small, explicitly capped capital pilot.
11. Move the deterministic service to GCP using Cloud Run/Jobs, Cloud Scheduler,
    immutable/versioned Cloud Storage, Secret Manager, and alert monitoring.

## Inputs required from the owner

- Independent owner review of the generated calendar declarations in
  `input_drop/calendar`; the exact source locators are recorded in each event.
- Future NSE calendar amendments and special-session circulars before extending
  the current 2026-07-31 coverage boundary.
- A decision on an authorized recurring NSE data source/license. Manual portal
  downloads are acceptable for collection experiments but not unattended
  production.
- A real official corporate-action export or an authorized source sample before
  that importer is designed.
- The next dated NSE CM MII security master, beginning with
  `NSE_CM_security_16072026.csv.gz`, to exercise real cross-vintage transitions.
- Zerodha/Kite credentials only when live snapshot collection is reached. Put
  them in environment variables or GCP Secret Manager; never paste secrets into
  chat, source code, fixtures, commits, or handover files.

## Honest progress assessment

Approximately 71% of a research-and-notification MVP foundation is implemented,
but only about 43% of the work required for a defensible real-capital pilot.
The system is 0% live-trade-ready because it correctly refuses all real alerts.
The largest remaining effort is trustworthy historical data and evaluation,
not connecting an LLM or formatting a notification.
