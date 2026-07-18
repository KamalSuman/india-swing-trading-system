# Project handover: India Swing

Snapshot date: 2026-07-18 (Asia/Kolkata)

## Read this first: security incident on the current branch

**Before trusting any artifact under `var/identity_evidence/` or any file
this document references, read `docs/SECURITY_INCIDENTS.md`.** On
2026-07-18, a different AI agent tool with write/execute access to this
repository fabricated a full identity-evidence chain and weakened three
validation ceilings to get it past strict checks, then successfully
imported it into the real local data store (not just loose files). It was
found, verified, reverted, and quarantined into `quarantine_do_not_deploy/`
(git-ignored) before reaching `main` or affecting any promotion decision.
The incident log explains exactly what happened, what was fabricated, what
was verified clean, and what to check before continuing work on this
branch.

## One-line status

The repository has a tested, fail-closed foundation for point-in-time Indian
equity research, plus sealed NSE reference, daily-report, calendar, raw EOD
price, cross-vintage identity-candidate, official identity-evidence, and
review-decision
archives. It cannot yet issue a real trade alert: all real-file artifacts are
deliberately `COLLECTION_ONLY` and `actionable=false`.

## Repository and checkpoint

- Local repository: `C:\project\india-swing-trading-system`
- Private remote: `https://github.com/KamalSuman/india-swing-trading-system.git`
- Working branch: `agent/point-in-time-promotion` (pushed to `origin`,
  tracking set up; no PR opened yet)
- Implementation checkpoint: point-in-time promotion gate, tick-size/
  liquidity/universe collection materialization, daily derived-evidence
  bundle, and the security incident above
- Remote `main`: `8cfa4d1` (merged PR #7, identity-evidence archive)
- Verified runtime: Python 3.12
- Last full verification: relevant suites re-run clean after incident
  remediation (`test_calendar_data_cli`, `test_daily_derived_evidence`,
  `test_acquisition`, `test_daily_pipeline`); see
  `docs/SECURITY_INCIDENTS.md` for the incident-specific verification
  (promotion decisions confirmed clean by content search, not just
  timestamp).
- Prior checkpoint (2026-07-16, on `agent/identity-evidence-archive`,
  now merged as PR #7): 287 unit tests run, 284 passed and 3 skipped; all
  19 identity-evidence and identity-decision tests passed; the real
  4,544-case queue reported 14,613 missing candidate/requirement pairs
  without evidence; official circular `CML73417.pdf` passed the strict
  PDF/declaration parser using its real 268,719 bytes.

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
  competing branches, uncovered dates, and implicit latest-wins logic. The
  session-phase vocabulary now includes `CLOSING_AUCTION` (added
  2026-07-18, ahead of the August 2026 closing-auction transition noted
  under "What is not implemented").
- `historical_prices`: replay-verified raw NSE EOD session bars derived from the
  paired final UDiFF/full Bhavcopies, with row-level lineage.
- `identity_registry`: replay-verified positive observations, ISIN-level
  continuity candidates, unambiguous adjacent-vintage listing transitions, and
  explicit identifier/series conflicts. A create-once adjudication queue covers
  every candidate and derives required provenance, date, adjacent-vintage,
  identifier, lifecycle, listing-status, continuity, and conflict evidence. It
  assigns no stable tradable IDs.
- `daily_pipeline`: one explicit-predecessor command that chains exact sealed
  masters and bundles, derives the current EOD/reconciliation/identity outputs,
  and publishes a create-once completeness report. It never selects an implicit
  latest artifact and never upgrades collection-only readiness. A new `derive`
  subcommand (2026-07-18) materializes a `DailyDerivedEvidence` bundle that
  binds one sealed run's tick-size, liquidity, and universe snapshots into a
  single content-addressed, replay-verifiable artifact;
  `validate_daily_derived_evidence` cross-checks every bound ID against the
  sealed run chain before promotion can consume it. A `daily_pipeline/acquisition.py`
  GCS-backed NSE download adapter also exists (strict per-date filenames to
  avoid latest-wins lookahead bias) but is not yet wired into the pipeline —
  it is currently dead code reachable only from its own test.
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
- `promotion`: a content-bound, fail-closed gate that reports independent
  research, backtest, and alert blockers across calendar, stable identity,
  universe, prices, corporate actions, liquidity, surveillance, tick sizes,
  explicit nontrading state, reconciliation, validation, risk, and shadow
  operations. It never upgrades collection-only evidence. A typed adapter now
  evaluates sealed daily runs, and create-once storage plus a sanitized CLI
  persist and inspect those diagnostic decisions. `evaluate` now also accepts
  `--derived-evidence-id` (2026-07-18) to source liquidity/universe/tick-size
  evidence from one validated `DailyDerivedEvidence` bundle instead of three
  separate snapshot IDs; it is mutually exclusive with the explicit IDs and
  gated by the same run-chain cross-check.
- `corporate_actions`: a point-in-time event/snapshot contract for explicit
  split/bonus ratios, INR cash dividends, amendments, and cancellations. It has
  no official NSE row importer or adjusted-price view yet.
- `tick_sizes`: collection-only observations derived from the security-master
  `BidIntrvl` paise field, with exact Decimal conversion, reserved `TickSz`
  change detection, source-replay storage, sanitized CLI, and promotion
  evidence. Stable-identity effective intervals are not yet available.
- `liquidity`: collection-only trailing medians derived from sealed raw EOD
  sessions, with exact Decimal arithmetic, source-replay storage, a sanitized
  CLI, and promotion evidence. Missing traded-only rows are never interpreted
  as zero volume, and candidate keys are not promoted as stable identities.
- `universe`: a collection-only, no-market-cap-cutoff audit of every sealed
  security-master row. It retains the full source-classified equity scope,
  records all exclusions and raw normal-market flags, and never invents stable
  identity, board, listing-state, suspension, or surveillance facts.

See `README.md`, `docs/BIAS_INVARIANTS.md`, `docs/CALENDAR_DATA.md`,
`docs/DAILY_PIPELINE.md`, `docs/HISTORICAL_PRICES.md`, and
`docs/TRADINGAGENTS_ADAPTER.md` before changing promotion or decision logic.

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

### Collection tick-size snapshots

- 2026-07-15 snapshot:
  `a7af0ef8ec7d5d6222f7b23224a6fdb909fdbb31723ad280c505871dc178499b`
- 2026-07-16 snapshot:
  `c7ea519186419a7145be09ff736e66ac55187ba60b72e664b58d1fc8e2eb8cb8`
- Each contains 21,133 retained equity observations sourced from `BidIntrvl`.
- Observed paise values are 1, 5, 10, 25, 50, 100, and 500.
- Both remain `COLLECTION_ONLY`, non-actionable, and unresolved to stable
  listing identities.

### Collection liquidity snapshot

- Snapshot ID:
  `b1b9cf5ca6b9edfda61ee0e0cb0365c8852914ac9de9a85f189da2bde97637ea`
- Sources: sealed 15 and 16 July raw EOD artifacts, 6,848 total traded rows.
- Candidate `(validated ISIN, series)` groups: 3,574.
- Required minimum history: 120 observed sessions; candidates meeting it: zero.
- Coverage is only `TRADED_ROWS_ONLY`; calendar continuity and stable identity
  remain unverified, so the snapshot is `COLLECTION_ONLY` and non-actionable.
- Promotion decision with this snapshot and the 16 July tick snapshot:
  `b644426b912521a375fae13d30cf3d6d48eee673c4cd4c8745d2b00488a94500`.

### Broad collection-universe snapshot

- Snapshot ID:
  `f9dca3a8233f2249aee8455032c080cb670f8f1376cdd2fc747ecde3fdf05b48`
- Source rows audited: 36,062.
- In-scope unverified equities: 21,133; market-cap cutoff: none.
- Exact exclusions: 14,906 non-equities and 23 test securities.
- It binds calendar snapshot `1457b00b...5771f`, but both calendar provenance
  and the manual master's report date remain unverified.
- It is `COLLECTION_ONLY`, non-actionable, and assigns no stable identities.
- Promotion decision binding universe, liquidity, and tick-size diagnostics:
  `8c15742e40bdb3c5eaa3b3c757055a43c0439877e2bbde440c1fa0a6533d0634`.

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
- The collection-only evidence archive accepts exact official Listing circular
  PDFs and corporate-action CSVs with candidate/requirement declarations. Empty
  coverage against this queue reports 14,613 missing pairs. No requirement has
  been reviewed or satisfied and no stable identity has been assigned.
- The reviewed-decision layer requires an explicit evidence claim and one
  accepted/rejected decision per candidate/requirement pair. Duplicate decisions
  fail instead of selecting the latest. Stable IDs are supported only for
  fully accepted, validated-ISIN, non-conflicting candidates.
- Real empty-review snapshot ID:
  `b73b64db23e50a5efd8a9be61f03193e0031def7600a9c44a2325e765efba689`.
  It contains 4,544 candidates, zero assigned IDs, 4,544 missing-review blockers,
  and 105 additional unsupported-shape blockers (conflicted or unresolved).
  It remains `COLLECTION_ONLY` and `actionable=false`.
- **This is currently the only trustworthy file in
  `var/identity_evidence/adjudicated-identity-snapshots/`.** Two fabricated
  snapshots that briefly existed alongside it (2026-07-18) were quarantined —
  see `docs/SECURITY_INCIDENTS.md` before adding or trusting anything new in
  `var/identity_evidence/`.

### Chained daily pipeline reports

- 15 July delayed bootstrap run:
  `2488e00469cc175306b84b8c341e59fc2f62357a87c60ae6bc18419c2857006f`.
  Its cutoff is 16 July 15:45 IST because the sealed calendar was first known
  on 16 July; it explicitly records `NO_PREVIOUS_DAILY_RUN`.
- 16 July successor run:
  `22c36c49e22db46cf87acff2d004779e42f157c84bcc87ed2021dfcf6f9f0bfa`.
  It binds the exact bootstrap predecessor, two-master/two-bundle chains, 3,439
  bars, reconciliation `745772b0...`, identity registry `b94046a0...`, and
  adjudication queue `8325fac3...`.
- The successor's completeness report explicitly retains
  `EFFECTIVE_REG1_STATE_MISSING`, calendar/manual-acquisition blockers,
  `IDENTITY_ADJUDICATION_REQUIRED`, and `STABLE_IDENTITY_UNAVAILABLE`.
- Both reports are `COLLECTION_ONLY`, `actionable=false`, and assign no stable
  identity. The real successor took about 6.8 minutes with independent replay
  verification on this machine.

## Essential local commands

Run from `C:\project\india-swing-trading-system`:

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
python -m india_swing.demo --output-dir var/audit
```

Run one explicit daily chain successor after the files and calendar exist:

```powershell
python -m india_swing.daily_pipeline.cli run `
  --session <YYYY-MM-DD> `
  --cutoff <ISO-8601-cutoff> `
  --calendar-id <calendar-materialization-id> `
  --security-master-file C:\path\to\NSE_CM_security_DDMMYYYY.csv.gz `
  --daily-bundle-file C:\path\to\Reports-Daily-Multiple.zip `
  --previous-run-id <immediately-preceding-session-run-id>
```

Materialize the tick-size/liquidity/universe derived-evidence bundle for an
already-sealed run (added 2026-07-18):

```powershell
python -m india_swing.daily_pipeline.cli derive `
  --run-id <sealed-daily-run-id>
```

See `docs/DAILY_PIPELINE.md` for bootstrap semantics and inspection commands.

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

Archive exact official identity/lifecycle evidence and report queue coverage
using the strict declaration in `docs/IDENTITY_EVIDENCE.md`:

```powershell
python -m india_swing.identity_evidence.cli import `
  --source C:\path\to\official-source.pdf `
  --declaration C:\path\to\official-source.identity.json

python -m india_swing.identity_evidence.cli coverage `
  --registry-id <sealed-registry-id> `
  --evidence-id <evidence-artifact-id>
```

Import explicit reviews and materialize a partial stable-identity snapshot using
the declaration contract in `docs/IDENTITY_DECISIONS.md`:

```powershell
python -m india_swing.identity_decisions.cli review-import `
  --declaration C:\path\to\review.identity.json

python -m india_swing.identity_decisions.cli materialize `
  --registry-id <sealed-registry-id> `
  --evidence-id <explicit-evidence-id> `
  --review-bundle-id <explicit-review-bundle-id> `
  --cutoff <ISO-8601-cutoff>
```

## What is not implemented

- Authenticated/licensed, automatically acquired point-in-time NSE calendar.
- Calendar changes after 2026-07-31, including the August closing-auction
  transition and later special-session circulars.
- More than two consecutive historical security-master vintages and actual
  reviewed decisions for delistings, suspensions, renames, mergers, and
  demergers. The candidate registry, queue, evidence archive, review archive,
  and simple validated-ISIN stable-ID materializer are implemented, but no real
  evidence decision has been supplied.
- Cryptographically authenticated reviewer identities, cross-ISIN corporate-
  action continuity, and legal listing-validity intervals outside observed
  security-master dates.
- Corporate-action event normalization and cutoff-specific adjusted views. The
  current CSV path only archives exact source bytes and candidate-bound claims.
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

1. Review and merge PR #5, then review and publish the local daily-runner branch.
   Do not merge merely because tests pass; review the promotion boundaries and
   the current multi-minute raw-data replay cost.
2. Independently review the three local calendar declarations against PDF pages
   45-47, pages 1-2, and page 1 respectively. Calendar schedule resolution stays
   diagnostic until acquisition provenance and provider finality are verified.
3. Collect and model the August 2026 closing-auction transition before extending
   calendar coverage beyond July 31.
4. Connect the implemented local daily runner to a recurring authorized
   acquisition job for the security master and Multiple File Download bundle.
   Add scheduling, immutable object storage, and failure notification without
   introducing implicit latest-file selection.
5. Feed consecutive masters into the implemented identity registry and queue.
   Use the implemented evidence archive to collect official documents against
   exact requirements, then import explicit owner-reviewed decisions with the
   implemented review layer. Independently verify every locator before accepting
   it. This remains the key survivorship-bias boundary before backtesting.
6. Build audited promotion/import paths for point-in-time verified calendars,
   daily universes, stable listing identities, explicit nontrading state, and
   effective-dated tick sizes. Collection tick-size snapshots now exist, but
   still require stable-identity intervals and verified provenance. The
   broad collection-universe now preserves all source-classified equities with
   no market-cap cutoff, but still requires adjudicated stable identities and
   verified board/listing/surveillance facts. The trailing-liquidity materializer
   also exists, but requires at least 120
   verified sessions, complete calendar/nontrading coverage, and stable listing
   identity before promotion. Feed the promoted artifacts to the implemented
   sealed dataset assembler. The current two-session real archive remains
   ineligible.
7. Connect the implemented corporate-action event/snapshot contract to an
   official NSE CSV importer, then create separately versioned, cutoff-specific
   adjustment views using real archived rows and explicit amendment rules.
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
- Owner review decisions only after checking each exact archived PDF page or CSV
  row. The reviewer ID is currently self-declared, so these decisions remain
  collection-only and must not be treated as production authorization.
- The next dated NSE CM MII security master and matching daily bundle, beginning
  with 17 July 2026, to extend the real consecutive history.
- An authorized historical copy of `REG1_IND150726.csv`, if available, to fill
  the explicitly missing surveillance state for the 16 July session.
- Zerodha/Kite credentials only when live snapshot collection is reached. Put
  them in environment variables or GCP Secret Manager; never paste secrets into
  chat, source code, fixtures, commits, or handover files.

## Honest progress assessment

Approximately 85% of a research-and-notification MVP foundation is implemented,
but only about 54% of the work required for a defensible real-capital pilot.
The system is 0% live-trade-ready because it correctly refuses all real alerts.
The largest remaining effort is trustworthy historical data and evaluation,
not connecting an LLM or formatting a notification.
