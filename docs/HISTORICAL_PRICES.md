# Raw NSE EOD historical-price artifacts

Status: one-session materialization and a sealed local derived-artifact store are
implemented. The output is `COLLECTION_ONLY`, `actionable=false`.

## Source and coverage

The materializer uses the already sealed NSE Multiple File Download bundle and
requires exactly one paired, row-confirmed final UDiFF and full-delivery
Bhavcopy for the requested trade date. It fully reopens and reparses the raw ZIP
before deriving any bars.

Every UDiFF row becomes exactly one raw bar, including non-EQ series. Every
full-delivery row must attach to exactly one UDiFF listing key. Row numbers,
row hashes, report hashes, report manifests, source validation times, and the
complete daily-bundle manifest remain in lineage.

Materialize and seal one session with:

```powershell
python -m india_swing.historical_prices.cli materialize `
  --daily-bundle-id <sealed-daily-bundle-id> `
  --market-session 2026-07-15 `
  --cutoff 2026-07-15T20:30:00+05:30
```

The output deliberately declares:

- `price_basis=RAW_UNADJUSTED`;
- `coverage_scope=TRADED_ROWS_ONLY`;
- `readiness=COLLECTION_ONLY`;
- `actionable=false`.

Bhavcopy absence is not interpreted as delisting, suspension, ineligibility, or
non-membership. The session-scoped UDiFF financial instrument ID is not treated
as a permanent company identity.

## Point-in-time rules

The artifact knowledge time equals the sealed source bundle's successful
validation time. A cutoff before that timestamp is rejected; the trade date or
filename date is never substituted for availability. Equivalent timezone
cutoffs produce identical UTC-normalized content.

OHLC, previous close, last price, volume, traded value, trade count, board lot,
average price, and delivery fields retain exact decimal/integer semantics. Bars
are immutable and never retroactively adjusted.

## Sealed derived store

Each materialization is stored by content ID with a canonical manifest and
deterministic JSON payload. On every read, the store reopens the exact daily
bundle named in lineage, reparses its raw ZIP, rematerializes the requested
session and cutoff, and requires byte-for-byte equality. Path escapes,
links/junctions, duplicate partitions, unexpected files, and manifest/payload
tampering fail closed.

## Remaining boundary

This is not yet a survivorship-free backtest dataset. Bulk session enumeration
must come from the event-sourced calendar, and identity must come from historical
security-master vintages. Corporate-action notices must be separately archived
with publication knowledge time before any cutoff-specific adjusted-price view
is created. Splits or dividends must never rewrite these stored raw bars.
