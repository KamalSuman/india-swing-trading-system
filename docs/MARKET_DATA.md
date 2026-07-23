# Read-only broker market-data connectors

Status: implemented and fixture-tested; no real account credentials or live
snapshots have been used yet.

The market-data layer has one canonical historical request/candle/batch model
and provider-specific adapters. `KiteMarketDataAdapter` and
`UpstoxHistoricalDataAdapter` both implement the
`HistoricalDailyDataConnector` boundary. `HistoricalMarketDataCollector` can
therefore validate and persist either provider without changing downstream
research code.

The connectors expose reads only. They do not expose orders, portfolio mutation,
or broker execution.

## Supported contract

Shared historical contract:

- Every provider instrument ID is bound to an ISIN/listing key, exact validity
  interval, and one or more immutable point-in-time source snapshot IDs.
- The caller supplies a sorted, unique tuple of expected historical sessions.
  Missing, extra, or duplicate provider rows fail closed.
- Both provider responses are translated to the same Decimal-based canonical
  candle and content-identified batch models.
- The collector checks provider, request, version, and nested content identity
  before publishing an immutable snapshot.
- A current broker instrument list never defines the historical universe.

Kite:

- Kite backend API: v3.
- Pinned Python SDK: `kiteconnect==5.2.0`.
- Instrument master: collected once per day and archived as a distinct vintage.
- Daily candle: one explicitly requested session at a time, paced within one
  process to the documented three historical requests per second.
- The limiter does not coordinate concurrent CLI processes. Production must
  serialize Kite calls or use a shared account/API-key limiter.
- Transient GET failures use bounded retries.
- `TokenException` is never retried and requires a fresh login. Other `403`
  permission/plan failures are reported separately.
- Credentials are read only from runtime environment variables and are excluded
  from representations, content identities, CLI errors, and snapshot payloads.

Upstox:

- Historical Candle Data V3 with the `NSE_EQ|ISIN` instrument key.
- Daily interval only; long histories are split into conservative ranges below
  the documented ten-year maximum.
- Each HTTP response is size-bounded and SHA-256 recorded before normalization.
- JSON keys, types, timestamps, OHLC consistency, and exact requested-session
  coverage are validated strictly.
- Calls are conservatively paced at one request per second. `429` and transient
  availability failures use bounded retries; authentication, permission, and
  request failures are never retried.
- The standard-library HTTPS transport is used, so no Upstox SDK dependency is
  required.

Official references:

- [Kite instruments and quotes](https://kite.trade/docs/connect/v3/market-quotes/)
- [Kite historical candles](https://kite.trade/docs/connect/v3/historical/)
- [Authentication and access-token lifecycle](https://kite.trade/docs/connect/v3/user/)
- [Errors and rate limits](https://kite.trade/docs/connect/v3/exceptions/)
- [Official Python SDK](https://github.com/zerodha/pykiteconnect)
- [Upstox Historical Candle Data V3](https://upstox.com/developer/api-documentation/v3/get-historical-candle-data/)
- [Upstox instruments](https://upstox.com/developer/api-documentation/instruments/)
- [Upstox analytics token](https://upstox.com/developer/api-documentation/analytics-token/)
- [Upstox rate limits](https://upstox.com/developer/api-documentation/rate-limiting/)

## Important data limitations

Current Kite and Upstox instrument dumps are not survivorship-free historical
security masters. They contain currently tradable instruments, and provider
identifiers can change or be reused. We archive every daily vintage and use
`exchange:tradingsymbol` only as a listing key, not as a permanent
economic-security identifier.

Kite does not provide enough information in this dump to prove main-board vs
SME status, ASM/GSM status, suspension history, ISIN continuity, delistings, or
historical symbol mappings. No record becomes actionable until a dated official
security-master/surveillance enrichment supplies those fields.

The instrument dump's `last_price` is represented as `dump_last_price`. It must
never populate the pipeline's finalized EOD price. Trade prices come only from
separately collected, finality-validated candles.

The archive stores a typed, adapter-normalized representation derived from the
official SDK response. It is not the exact SDK return value and does not contain
the original gzipped HTTP CSV bytes. Arbitrary raw bytes are rejected; wire
capture stays disabled until it has a provider-specific schema and secret-
redaction contract.

Every candle archive names the exact instrument-master snapshot and listing key
used to resolve its numeric token. The adapter refuses to use today's token for
a date before that master vintage. Safe historical backfill therefore requires
the dated NSE security-master/ISIN lineage. Current Kite tokens or an Upstox BOD
instrument file alone are not treated as proof of historical identity.

## Authentication

### Kite

Historical candles require a paid Kite Connect plan. The Zerodha account must be
active and have TOTP enabled before the official interactive login flow can be
completed. The collector requires:

```text
INDIA_SWING_KITE_API_KEY
INDIA_SWING_KITE_ACCESS_TOKEN
```

For a normal retail login, the access token expires at 06:00 the following day.
It can be invalidated earlier by logout or other session changes. This project
does not automate passwords or TOTP. Do not paste credentials into source files,
command history, logs, issues, or chat.

Install the optional pinned dependency using the verified Python 3.12 runtime:

```powershell
python -m pip install -e ".[kite]"
```

Collect the current NSE instrument vintage and retain the returned snapshot ID:

```powershell
$env:INDIA_SWING_KITE_API_KEY = "runtime-value"
$env:INDIA_SWING_KITE_ACCESS_TOKEN = "daily-runtime-value"
india-swing-market-data instruments --exchange NSE
```

Collect one finalized daily candle:

```powershell
india-swing-market-data daily `
  --instrument-master-snapshot-id <id-from-instruments-command> `
  --instrument-token 408065 `
  --session 2026-07-15 `
  --exchange NSE
```

The CLI does not accept a caller-provided finality timestamp. The current code
uses a fixed regular-session guard: 15:30 IST close and a 16:00 IST data-ready
floor. That guard is deliberately **collection-only and non-actionable**. It
cannot prove holidays, special sessions, or Muhurat timings. A candle becomes
eligible for trading decisions only after a dated, versioned official NSE
calendar supplies the actual session contract. Missing, malformed, duplicate,
wrong-session, or wrong-timezone output fails closed and publishes no snapshot.

The existing `india-swing-market-data` CLI remains the single-session Kite
collector. Multi-session provider-neutral history is exposed through the Python
connector boundary and requires the caller to supply a versioned session set and
point-in-time instrument binding.

### Upstox

Provide a runtime bearer token or read-only analytics token:

```text
INDIA_SWING_UPSTOX_ACCESS_TOKEN
```

Construct `HistoricalInstrumentBinding` from dated NSE lineage, then create a
`HistoricalDailyRequest` containing the exact sessions expected from the
versioned trading calendar. Pass the request to
`HistoricalMarketDataCollector(UpstoxHistoricalDataAdapter(...), store)`.

The adapter accepts only an Upstox binding whose provider instrument ID is
exactly `NSE_EQ|<binding ISIN>`. It never lists instruments, selects a "latest"
identifier, expands the requested universe, or silently accepts additional
sessions. Production credentials and the live network are never activated by
tests.

After installing the package, the operational entry point is
`india-swing-upstox-backfill`. From a source checkout, including the current
development virtual environment, use the equivalent module form:

```powershell
.\.venv\Scripts\python.exe -m india_swing.market_data.backfill_cli --help
```

Planning is credential-free and requires exact pinned input IDs plus an explicit
knowledge timestamp. First fetch and seal the public Upstox NSE BOD catalog:

```powershell
.\.venv\Scripts\python.exe -m india_swing.market_data.backfill_cli catalog-fetch
```

The command stores the original `NSE.json.gz` and a canonical normalized catalog
under `var/market_data/upstox-nse-instrument-catalog/<catalog-id>`. Reads replay
the normalized catalog from the raw gzip and reject path, content, or canonical
encoding changes. No Upstox credential is read. A manually downloaded file can
instead be sealed with `catalog-import --source-file <path> --observed-at
<aware-ISO-datetime>`.

Use the returned catalog ID and a `requested-at` timestamp at or after its
`observed-at`:

```powershell
.\.venv\Scripts\python.exe -m india_swing.market_data.backfill_cli plan `
  --identity-registry-id <registry-id> `
  --identity-snapshot-id <optional-exact-reviewed-snapshot-id> `
  --calendar-materialization-id <calendar-materialization-id> `
  --upstox-catalog-id <catalog-id> `
  --coverage-start 2026-07-15 `
  --coverage-end 2026-07-16 `
  --requested-at 2026-07-23T10:00:00+00:00
```

Reuse the identical arguments for every resume. Changing `requested-at`, an
input ID, or a coverage bound intentionally creates a different plan ID.

`--identity-snapshot-id` is optional and is never selected by directory order,
modification time, or a "latest" lookup. When supplied, it must be a
content-verified adjudicated snapshot for the exact registry and must have been
known by `--requested-at`. Reviewed corrected identifiers and reviewed conflict
cases may then contribute their exact effective ISIN. The plan binds the
snapshot ID into its content identity and into request source lineage.
Unreviewed claims, incomplete or rejected cases, listing mismatches, and
snapshots known after the requested time remain blocked.

Run one request as a live smoke test:

```powershell
$secureToken = Read-Host "Upstox token" -AsSecureString
$env:INDIA_SWING_UPSTOX_ACCESS_TOKEN = `
  [System.Net.NetworkCredential]::new("", $secureToken).Password

.\.venv\Scripts\python.exe -m india_swing.market_data.backfill_cli run `
  --identity-registry-id <registry-id> `
  --identity-snapshot-id <optional-exact-reviewed-snapshot-id> `
  --calendar-materialization-id <calendar-materialization-id> `
  --upstox-catalog-id <catalog-id> `
  --coverage-start 2026-07-15 `
  --coverage-end 2026-07-16 `
  --requested-at 2026-07-23T10:00:00+00:00 `
  --maximum-requests 1
```

The run command refuses plans containing blocking coverage or identity issues
before it reads credentials. `--allow-collection-with-issues` is available only
for explicitly partial collection; its output continues to report
`coverage_complete: false`.

## Historical backfill planning and restart

`build_historical_backfill_plan` combines an exact cross-vintage identity
registry, a pinned NSE CM calendar, a provider instrument resolver, a requested
date interval, and a knowledge timestamp.

The current identity registry contains positive observations and unverified
claimed report dates; it cannot prove absence, listing inception, or delisting.
Accordingly, the planner remains collection-only and:

- creates a request only when that security has an exact positive observation
  on every session in the request run;
- never fills a missing security-master vintage by carrying the previous or next
  identity forward;
- admits only the intended `EQ` main-board and `SM` SME listing lanes;
- requires the dated row to have `DelFlg=N`, normal-market status `6`, and
  normal-market eligibility `1`;
- separates symbol and series lanes;
- excludes retained deleted aliases, suspended rows, migrated-out SME rows, and
  other normal-market-ineligible observations without deleting their evidence;
- evaluates identity collisions among concurrently eligible observations on
  each exact report date, rather than allowing a deleted historical alias to
  block the active same-ISIN listing forever;
- blocks duplicate active ISIN/series rows, financial-instrument-ID reuse,
  listing-key reuse across identifiers, and concurrent eligible lanes that
  collapse to one provider key;
- records missing master dates, conflicts, unvalidated identifiers, delete
  flags, normal-market ineligibility, unavailable provider keys,
  current-catalog absence, and unsupported listing lanes as immutable plan
  issues.

Issue severity is explicit. Missing master vintages, identity conflicts,
unvalidated identifiers, ambiguous keys, and unavailable routing block normal
collection. Deleted securities, normal-market-ineligible rows, and unsupported
series are expected exclusions. This exclusion is point-in-time: it is based
only on the exact dated master row and never on today's provider membership.
An ISIN absent from today's Upstox catalog is a warning, not an exclusion:
current BOD files omit delisted instruments, so using current membership to
filter historical NSE membership would introduce survivorship bias. The request
still uses the deterministic `NSE_EQ|<historical validated ISIN>` key; any
upstream rejection remains visible during collection.

### Blocker work list

Generate a content-addressed report containing only genuine blockers and their
existing identity-adjudication cases:

```powershell
.\.venv\Scripts\python.exe -m india_swing.market_data.backfill_cli blockers `
  --identity-registry-id <registry-id> `
  --calendar-materialization-id <calendar-materialization-id> `
  --upstox-catalog-id <catalog-id> `
  --coverage-start 2026-07-15 `
  --coverage-end 2026-07-16 `
  --requested-at 2026-07-23T21:05:00+05:30
```

The report is sealed below
`var/market_data/historical-backfill-blocker-reports/<report-id>/report.json`.
It excludes normal deletion/series exclusions and current-catalog warnings. Each
remaining issue is bound to its exact observation IDs, candidate IDs, existing
adjudication case IDs, evidence requirements, and operator actions. It remains
`actionable=false` and `evidence_satisfied=false`; it routes work into the
official identity-evidence process but cannot invent an identity decision.

Turn that exact report into a sealed procurement package and a
spreadsheet-friendly work list:

```powershell
.\.venv\Scripts\python.exe -m india_swing.market_data.backfill_cli evidence-worklist `
  --blocker-report-id <exact-blocker-report-id>
```

The command loads only the explicitly named blocker report and its exact
registry/queue lineage. It writes:

- `package.json`, the canonical content-identified system artifact; and
- `worklist.csv`, one human-readable row per candidate observation and required
  evidence pair.

Both files live below
`var/market_data/historical-backfill-evidence-work-packages/<package-id>/`.
The CSV includes the observed symbol, series, name, report date, source
identifier, validated ISIN when present, issue/case IDs, exact requirement,
recommended official NSE document types, and required operator actions. A
missing-vintage or other operational blocker remains a separate operational
row; it is never forced into a fabricated identity case.

Document recommendations distinguish dated security masters, adjacent
security-master vintages, report-date provenance, listing circular PDFs, and
corporate-action CSVs. They are procurement guidance only. Both
`evidence_collected` and `review_completed` are emitted as false, and the
package remains `actionable=false` and `evidence_satisfied=false`. Editing
either JSON or CSV makes the stored package fail integrity verification.

`HistoricalBackfillRunner` executes the safe requests through the same
provider-neutral collector. After every request it atomically writes a
content-identified local progress document. A restart verifies all completed
snapshots before proceeding. If a process stopped after publishing a snapshot
but before updating progress, the runner discovers the exact request snapshot
and records it without another provider call.

The local progress store is deliberately single-runner. A Cloud Run deployment
must replace its atomic local file update with generation-matched Cloud Storage
or another compare-and-swap store before multiple workers are allowed.

## Provider-versus-NSE reconciliation

An API success is not treated as proof that a candle is correct. The
reconciliation command compares provider OHLCV exactly against separately
materialized NSE UDiFF/full-bhavcopy session artifacts:

```powershell
.\.venv\Scripts\python.exe -m india_swing.market_data.backfill_cli reconcile `
  --provider UPSTOX `
  --provider-snapshot-id <historical-provider-snapshot-id> `
  --nse-artifact-id <exact-NSE-session-artifact-id> `
  --reconciled-at 2026-07-23T10:30:00+00:00
```

Supply one `--nse-artifact-id` for every provider session. Missing or extra
sessions fail before comparison. Listing symbol, security series, and ISIN must
identify exactly one NSE bar. Open, high, low, close, and volume are compared as
exact Decimal/integer values. A mismatch or missing NSE row produces a persisted
failed report and process exit code 4. A passing report is also persisted, but
remains non-actionable and cannot itself authorize a trade.

## Local snapshot semantics

Each snapshot is content-addressed below `var/market_data`. Publication uses a
fully flushed temporary directory followed by a same-filesystem atomic rename.
Readers verify the complete manifest/path partition, typed payload hash, codec
version, semantic selection key, and derived record count. Secret-named fields,
cookie/auth headers, floats in normalized records, and arbitrary byte payloads
are rejected before a directory is created.

An identical retry resolves to the existing content ID; earlier valid vintages
are never overwritten by the store API. A local filesystem administrator can
still delete or replace files, so production requires conditional Cloud Storage
writes, retention controls, and access logs.

Historical selection requires all four inputs:
`latest_at_or_before(dataset, selection_key, cutoff, max_age=...)`. A snapshot
first observed after the decision cutoff, belonging to another semantic key, or
older than the explicit freshness bound cannot be selected. Candle consumers
should normally retain and load the exact snapshot ID in the decision lineage.
