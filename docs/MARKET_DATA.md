# Read-only Kite market-data collection

Status: implemented and fixture-tested; no real account credentials or live
snapshots have been used yet.

The collector wraps Zerodha's official `kiteconnect` Python SDK and exposes only
the daily instrument master and one-session historical daily candles. It does
not expose orders, portfolio mutation, or broker execution.

## Supported contract

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

Official references:

- [Kite instruments and quotes](https://kite.trade/docs/connect/v3/market-quotes/)
- [Kite historical candles](https://kite.trade/docs/connect/v3/historical/)
- [Authentication and access-token lifecycle](https://kite.trade/docs/connect/v3/user/)
- [Errors and rate limits](https://kite.trade/docs/connect/v3/exceptions/)
- [Official Python SDK](https://github.com/zerodha/pykiteconnect)

## Important data limitations

The current Kite instrument dump is not a survivorship-free historical security
master. It contains currently tradable instruments, and numeric tokens can be
reused. We archive every daily vintage and use `exchange:tradingsymbol` only as
a current listing key, not as a permanent economic-security identifier.

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
the planned dated NSE security master/ISIN mapping; current Kite tokens alone are
not treated as proof of historical identity.

## Authentication

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

Arbitrary date ranges and historical backfills are intentionally not accepted by
this first contract. Zerodha's formal API documentation does not publish a
current maximum date span or candle count per request, and a multi-session range
cannot be completeness-checked without a versioned trading calendar. A future
backfill path must chunk explicitly and verify expected sessions, ordering,
overlaps, lineage, and shared rate limiting.

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
