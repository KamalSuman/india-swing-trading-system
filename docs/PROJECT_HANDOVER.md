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
- Working branch: `agent/import-2026-07-16`
- Implementation checkpoint: 16 July import, replay, and reconciliation compatibility
- Remote `main`: `c684969` (merged PR #4)
- The working branch has no upstream and is not on GitHub at this snapshot.
- Verified runtime: Python 3.12
- Last full verification before this data checkpoint: 287 unit tests run, 284
  passed and 3 skipped. Current focused verification: all 11 reconciliation
  tests passed; touched-source `compileall`, real 16 July replay, and
  `git diff --check` passed.

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
  explicit identifier/series conflicts. A create-once adjudication queue covers
  every candidate and derives required provenance, date, adjacent-vintage,
  identifier, lifecycle, listing-status, continuity, and conflict evidence. It
  assigns no stable tradable IDs.
- `evaluation`: immutable expanding purged walk-forward folds over a versioned
  trading-session tuple, plus create-once content-addressed trial
  preregistrations with same-family parent lineage and append-only lifecycle
  chains for holdout access and outcomes. Its engine generates fills, itemized
  costs, mark-to-market equity, fixed metrics, and threshold results while
  rejecting training/validation signals and collection-only data. Full results
  are create-once artifacts. Strategy and benchmark now run through identical
  base/stressed paths, and their persisted comparison is required before
  lifecycle completion. Content-bound close-momentum strategy and liquid
  equal-weight benchmark generators create next-session intents across every
  registered test fold, with explicit as-of evidence and candidate vetoes.
  Each trial role has one create-once batch, fold metrics are recomputed from
  its equity evidence, and a frozen fold-sign/Holm gate covers complete trial
  families without accepting caller-supplied probabilities. Exact family
  snapshots are create-once artifacts; eligible trials can receive a separate
  post-completion research-promotion event only when it matches the completed
  comparison. Each trial has one create-once run manifest. A content-bound
  Markdown family/fold report is create-once and can be published, listed, or
  shown through a sanitized CLI. A sealed dataset assembler now requires exact
  daily calendar/universe/price bindings, adjudicated stable identities,
  explicit nontrading evidence, and effective-dated tick sizes. Its derived
  datasets and instruments have a create-once content-addressed local store;
  collection-only artifacts remain inadmissible.
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

### NSE security masters

- 2026-07-15: artifact `726f6c1ff4588cee1c072d6769035ed9035c5310f98321409df5ab6e3cd1efc4`,
  manifest `466d74e5c1062f680518c3651726ee2bb7a56d885db129b83eca851c3827b4c0`.
- 2026-07-16: artifact `9ea03e4108c8811204a810644462e6cd378de241ed4c1915733ff835c334b1e6`,
  manifest `e87670d62a14a5ac67c500b35221b0f7a13a3d622879cf7ce12eed9698ccbcdf`.
- Each master contains 36,062 parsed rows and 21,133 retained unverified
  equity rows.

### NSE daily bundles

- 15 July artifact: `44e2079041e3b05a43703bc63e030d4ebce44b2cb05d4209177adc7431844b6b`;
  24,609 selected rows spanning the 14 and 15 July final reports.
- 16 July artifact: `02d0426628ac268c29bec7bba334ff839d819cbe1773322770009235e27152eb`;
  manifest `6ccbfe5dac70d2697a26778bebfdee4f6f7b510f4b8256071e5dd0aa8c08efc5`;
  13,551 selected rows across all seven required report families.
- The browser-renamed 16 July source `(1).zip` was imported through an exact
  temporary `Reports-Daily-Multiple.zip` basename; its bytes were not modified.

### Positive traded-date evidence

- Artifact `92cdc918f207226eb0137bd59f83cc1ce9cb72b71b16de060fa7fd64033e05c1`:
  observed dates 2026-07-14 and 2026-07-15.
- Artifact `41f6dd965da951f8e53948e8088f8dad4a07f4fe7ed83e960ff9e6a2bcb7f4ba`:
  observed date 2026-07-16.

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

- Latest snapshot ID: `745772b03c971bfb97da7b2772516e45dbd0e862b5c9b2a8fcc3cfd93c1129a6`
- Target session: 2026-07-16, using the exact 15 and 16 July bundles.
- Retained master rows: 21,133
- Broad EQ scope: 3,510, including small caps
- SM watch-only scope: 772
- Other explicitly unsupported series: 16,851
- Same-session retained rows with trade evidence: 2,855
- Daily-report orphan keys retained as orphans: 4,031
- Supported rows unresolved after calendar resolution: 4,282
- The 15 July bundle contains `REG1_IND140726.csv`, not the missing 15 July
  REG1 publication needed for 16 July effective state. The snapshot therefore
  retains `EFFECTIVE_REG1_STATE_MISSING` rather than backfilling from 14 or 16 July.
- One official descriptive-name difference is retained as evidence for
  `OBCL:EQ`; instrument ID, ISIN, listing key, and board lot agree.
- Actionable rows: zero

### Raw EOD historical-price sessions

- 2026-07-15: artifact `ebc8a722e47fb9bc52b0c118550b85daf8f714f224d3feadb7a8f64a9e194c7f`,
  manifest `5cbcf5e581533188edf478c52d8d1614ab9ad6bb461c3a080b215d38c343b277`,
  3,409 bars and 3,256 delivery rows.
- 2026-07-16: artifact `43f7f2f262e09d98e5f124b173e08cfc60fe57d3f1d5c995b69e706dcb988831`,
  manifest `f1db2d5ae49f3d708d5de167aaae4452142e97e193633c4c3ab6bc1dbdb3f6a6`,
  3,439 bars and 3,275 delivery rows.
- Price basis: `RAW_UNADJUSTED`
- Coverage: `TRADED_ROWS_ONLY`
- Readiness: `COLLECTION_ONLY`; actionable: false

### Cross-vintage identity baseline

- Source vintages: two consecutive masters, claimed 2026-07-15 and 2026-07-16.
- Registry ID: `b94046a0e7deca6504875793262faab2411c61843d8f480efef3652dbde6c724`
- Manifest ID: `242344904187399b822db71a8497dbe2ed86adfa8b263040ced41ae93f4bd8c2`
- Positive observations: 42,266
- ISIN/unvalidated identifier candidates: 4,544
- Quarantined conflicts: 36
- Adjacent-vintage transitions: 20,998
- Stable identities assigned: zero
- Readiness: `COLLECTION_ONLY`; actionable: false
- Adjudication queue ID: `8325fac32053b4e9f34142eb46d00b40e2f41a56bdbeb73d55d0f2deccc0efe9`
- Adjudication cases: 4,544; stable identities assigned: zero.
- Required for all cases: authorized provenance and report-date verification.
- Additional requirements: 4,439 official continuity confirmations, 888
  listing-status cases, 92 adjacent-vintage observations, 92 validated
  identifiers, 13 official conflict resolutions, and one listing-lifecycle case.

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

Materialize and inspect the complete official-evidence work queue:

```powershell
python -m india_swing.identity_registry.cli adjudication-materialize `
  --registry-id <sealed-registry-id>

python -m india_swing.identity_registry.cli adjudication-show `
  --registry-id <sealed-registry-id>
```

## What is not implemented

- Authenticated/licensed, automatically acquired point-in-time NSE calendar.
- Calendar changes after 2026-07-31, including the August closing-auction
  transition and later special-session circulars.
- More than two consecutive historical security-master vintages and official
  evidence import/decisions for delistings, suspensions, renames, mergers,
  demergers, and stable instrument/listing IDs. The candidate registry and
  complete evidence queue are implemented, but two positive daily observations
  cannot by themselves establish legal/economic continuity.
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
- A report over actual point-in-time verified historical folds. The assembler
  exists, but no real upstream artifact currently satisfies its admission gate.
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
5. Feed consecutive masters into the implemented identity registry and its
   complete adjudication queue. Acquire the official evidence named by every
   case, then implement evidence import and reviewed decisions for stable
   effective-dated IDs. This remains the key survivorship-bias boundary before
   backtesting.
6. Build audited promotion/import paths for point-in-time verified calendars,
   daily universes, stable listing identities, explicit nontrading state, and
   effective-dated tick sizes. Feed them to the implemented sealed dataset
   assembler. The current two-session real archive remains ineligible.
7. Add an official corporate-action source using a real archived fixture;
   design its schema from the source rather than guessing it.
8. Evaluate the implemented deterministic baseline on point-in-time verified
   history with realistic Indian costs, slippage, liquidity, delistings, and
   registered trials.
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
- The next dated NSE CM MII security master and matching daily bundle, beginning
  with 17 July 2026, to extend the real consecutive history.
- An authorized historical copy of `REG1_IND150726.csv`, if available, to fill
  the explicitly missing surveillance state for the 16 July session.
- Zerodha/Kite credentials only when live snapshot collection is reached. Put
  them in environment variables or GCP Secret Manager; never paste secrets into
  chat, source code, fixtures, commits, or handover files.

## Honest progress assessment

Approximately 83% of a research-and-notification MVP foundation is implemented,
but only about 52% of the work required for a defensible real-capital pilot.
The system is 0% live-trade-ready because it correctly refuses all real alerts.
The largest remaining effort is trustworthy historical data and evaluation,
not connecting an LLM or formatting a notification.
