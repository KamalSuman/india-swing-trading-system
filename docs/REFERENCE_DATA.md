# NSE reference-data boundary

Status: contracts, synthetic tests, and a collection-only NSE CM MII security
master importer are implemented. No imported market-data artifact is committed
to the repository, and no surveillance file, holiday circular, or live universe
has been materialized.

The pipeline now requires content-addressed calendar and universe artifacts. A
current Kite instrument dump remains inventory-only and cannot be labelled
point-in-time verified or become actionable.

## Official source map

The safest free starting point for dated NSE cash-market membership is the daily
**CM - MII - Security File (.gz) (NSE Listed securities)** on
[NSE All Reports](https://www.nseindia.com/all-reports). Its dated filename is
`NSE_CM_security_DDMMYYYY.csv.gz`. NSE introduced this dissemination in
[circular MSD60315](https://nsearchives.nseindia.com/content/circulars/MSD60315.pdf)
from 5 February 2024. The current field contract is described in the
[NSE Masters Data specification](https://nsearchives.nseindia.com/web/mediaattachment/2026-04/NSE-Masters_Data-v1.8_20260428121249.pdf).
The exact 120-column ISO-tag CSV order is specified in Annexure 10 of PART-D in
[NSE capital-market consolidated circular CMTR73927](https://nsearchives.nseindia.com/content/circulars/CMTR73927.zip).

The report catalogue shows the human-facing display name rather than the
downloaded filename. Search for `MII` or `Security File`, select the dated
**NSE Listed securities** entry, and keep the `.csv.gz` compressed. Do not select
the separate **NSE Listed and BSE Exclusive securities** interoperability entry.

## Implemented manual import boundary

The reference-data CLI accepts exactly one manually downloaded NSE-only file:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.reference_data.cli security-master import `
  --file C:\path\to\NSE_CM_security_DDMMYYYY.csv.gz
```

The importer does not download or scrape anything. It:

- opens one regular non-link file descriptor, verifies its identity before and
  after the bounded read, and rejects path swaps or concurrent mutation;
- rejects corrupt, concatenated, trailing, non-UTF-8, or unknown-schema gzip data;
- pins the current 120-column ISO-tag header and exact dated filename pattern;
- validates every row and rejects duplicate instrument IDs or symbol-series keys;
- rejects interoperability content carrying BSE-exclusive alternative-venue rows;
- rejects nonblank values in ISO scope/type fields that are blank under the
  currently pinned NSE cash-market schema;
- preserves every source field and assigns exactly one auditable row disposition;
- preserves the raw source identifier while exposing an ISIN only when its
  12-character structure and check digit validate;
- stores the original bytes, deterministic normalized JSON, hashes, row digest,
  parser/schema/policy versions, and internally observed availability times;
- durably publishes the local artifact atomically under a process-released
  advisory lock and verifies it again on every read;
- remains `COLLECTION_ONLY` and `actionable=false`.

The filename date is stored as `claimed_report_date`, never as historical
knowledge time. The CSV has no internal report-date control row, so a local file
cannot independently prove that date or its origin. Its acquisition mode is
therefore `UNVERIFIED_MANUAL_FILE`, `verified_report_date` remains null, and
freshness selection refuses to use the filename claim. The archive is
partitioned by successful validation date. The manual public channel rejects
claimed dates before 5 February 2024 and implausibly far-future filenames.
Re-importing identical content keeps the earliest stored artifact; conflicting
bytes for the same claimed date fail closed under an atomic per-date import lock.
An authorized downloader or acquisition receipt must establish source URL,
retrieval evidence, and a verified report date before point-in-time promotion.

NSE master date/time integers use NSE's documented epoch of 1 January 1980, not
the Unix epoch. The importer deliberately preserves them as raw integers. It
also treats `BidIntrvl` as the paise-denominated tick field and does not substitute
the currently reserved ISO-tag `TickSz` column.

Daily surveillance/regulatory enrichment comes from `REG1_INDDDMMYY.csv` (and
the older `REG_INDDDMMYY.csv`). The consolidated REG1 file is generated after
market close and applies to the **next trading session**; that knowledge/effective
distinction is mandatory. Relevant official schema circulars include
[SURV64924](https://nsearchives.nseindia.com/content/circulars/SURV64924.zip),
[SURV65097](https://nsearchives.nseindia.com/content/circulars/SURV65097.zip),
and [SURV67801](https://nsearchives.nseindia.com/content/circulars/SURV67801.pdf).

The final UDiFF bhavcopy provides prices and trading evidence. It is not a
security master: an eligible zero-volume or suspended security can be absent, so
using bhavcopy membership as the universe would create survivorship bias.

Mutable current files such as `EQUITY_L.csv`, `SME_EQUITY_L.csv`, current
symbol/name-change CSVs, today's Kite instruments, and current ASM/GSM pages are
useful for validation but cannot be projected backward.

## Calendar source hierarchy

The human-facing source is NSE's
[market timings and holidays page](https://www.nseindia.com/resources/exchange-communication-holidays).
Its holiday JSON is an undocumented page backend, not a complete schedule.
Rows can include “Special Live Trading,” and a listed holiday can later receive
Muhurat timings. Calendar materialization must therefore be event-sourced:

1. versioned regular CM schedule;
2. annual holiday circular;
3. later closure/amendment circulars;
4. explicit special-session circulars with exact windows;
5. no mock/contingency session treated as live trading.

Examples of why overrides matter:

- [CMTR71775](https://nsearchives.nseindia.com/content/circulars/CMTR71775.pdf)
  published the 2026 base holidays while leaving Muhurat timings pending.
- [CMTR72260](https://nsearchives.nseindia.com/content/circulars/CMTR72260.pdf)
  added an ad-hoc January 2026 closure only days beforehand.
- [CMTR70319](https://nsearchives.nseindia.com/content/circulars/CMTR70319.pdf)
  defined nonstandard 2025 Muhurat timings.
- [MSD61893](https://nsearchives.nseindia.com/content/circulars/MSD61893.pdf)
  defined a Saturday live DR session with a nonstandard close.

The code requires one explicit `CalendarDay` for every covered date. Missing
dates, unknown special-session times, post-cutoff source vintages, or coverage
that does not reach the trade horizon fail closed.

## Implemented contract

The current reference contracts supply:

- external records with event time, knowledge time, source snapshot, and hash;
- eligibility records with separate half-open effective-session intervals,
  bound to the exact instrument/listing and supported state values;
- validity-dated listing mappings keyed by an opaque audited instrument ID and
  bound to an exchange and segment;
- explicit unknown states and one disposition for every scoped master row;
- collection-only, point-in-time-verified, and synthetic-test readiness states;
- complete calendar coverage with regular, special, holiday, weekend, and
  unscheduled-closure dates, including multiple real windows on split sessions;
- typed session-window phases: only `LIVE_CONTINUOUS` is executable, while
  pre-open, call-auction, and mock-test windows remain non-executable evidence;
- content-derived calendar/universe IDs;
- exact audited dataclass types throughout the reference graph, preventing a
  subclass from overriding session executability or effective-state resolution;
- pipeline checks that bind the decision, instruments, provider outputs,
  listing mappings, universe, calendar, session, and cutoff;
- full data- and instrument-content fingerprints on forecast, signal, setup,
  and research outputs, with exact provider-version checks;
- consumption-time identity verification and final pre-decision revalidation,
  so a provider cannot mutate a validated reference, candidate, component
  configuration, or risk policy mid-run;
- exact-one effective-state resolution for the signal and proposed entry
  sessions, including adjacent REG1-style half-open rollovers; overlapping or
  missing states fail closed, and a new suspension/surveillance state blocks
  entry even when the stock was eligible on the signal day;
- a next-session gate that requires listing validity to persist through entry
  and keeps entry/expiry inside one executable live-continuous window.

`POINT_IN_TIME_VERIFIED` construction remains deliberately disabled. The MII
importer now binds original archived bytes, the approved dataset kind, dated
filename, locally observed availability, parser/schema versions, row counts,
ordered row digest, and source hashes. It cannot establish authoritative
publication time, stable cross-vintage identity, REG1 surveillance, the trading
calendar, or liquidity completeness. Merely importing this file—or wrapping
Kite, bhavcopy, or hand-built rows in reference models—cannot enable real alerts.

Synthetic decisions carry `execution_eligible=false` inside the decision itself
and identify their reference readiness. The audit writer accepts only intact
typed pipeline results for pipeline-shaped records, binds the filename to the
run ID, rejects secret-bearing fields, and detects nested mutation after finish.
Even a result-only typed audit carries the trial, model bundle, data content,
source revision, execution-policy, and cost-schedule lineage fields.
These controls do not turn synthetic data into a real trading signal.

The current next-session persistence check is intentionally strict. Before live
use, the REG1 importer must materialize the entry-session state produced after
session D and a pre-alert/pre-entry revalidation must detect later exchange
changes. The code must not rewrite a report's source event or knowledge time to
make it appear effective earlier.

Synthetic tests exercise these contracts; they do not prove official historical
coverage. Current free dated masters appear defensible only from February 2024,
and consolidated REG1 coverage starts in December 2024. A longer reportable
backtest needs NSE's licensed historical Masters data or must be labelled
unsupported for the earlier period.

## Access and licensing

Do not add an NSE website scraper. NSE's
[Terms of Use](https://www.nseindia.com/static/nse-terms-of-use) restrict
systematic/automated collection, and its
[data policy](https://www.nseindia.com/static/market-data/nse-data-policy)
addresses automated/non-display usage for trading decisions. Public download
availability is not an automation licence.

The initial ingestion path should accept manually downloaded official artifacts
or an authorized/licensed feed, then archive the original bytes with retrieval
time, URL, media type, HTTP metadata, parser version, and SHA-256 before parsing.
Recurring automated acquisition requires an approved NSE channel or written
permission from NSE Data & Analytics.
