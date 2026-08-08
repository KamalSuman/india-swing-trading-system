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

### Legacy source-claimed ISIN lineage (v3)

For sessions imported from the official legacy Full Bhavcopy/MTO archive pair
(`nse-historical-archive-eq-session/v3`), the parser retains each row's exact
published ISIN and its exact data-row number alongside the record, as an
immutable, ordered `SOURCE_CLAIMED_UNVERIFIED` claim per EQ record. Every
claim is bound to its exact record and to the exact SHA-256 of the inner
Bhavcopy CSV bytes it was read from -- never the outer ZIP -- and is
independently re-verified, both at range-load time and at research-replay
time, before any session built from it is trusted.

This is a retained source claim, not a validated identity: it never sets
`validated_isin`, `financial_instrument_id`, or any identity-matched,
market-eligibility, feature-, training-, label-, alert-, or execution
authority. It remains unusable for identity resolution until a separately
pinned corroboration/admission decision promotes it. Sessions stored under
the earlier `v1`/`v2` schemas, and non-legacy `v3` sessions, carry no such
claims and never have one inferred or fabricated on replay.

### Research-only identity admission

`india_swing.evaluation.nse_archive_research_identity` is that separately
pinned admission decision. It streams one already-replayed research session
at a time and grades each EQ record into a deterministic, research-only
ISIN-based join key ("research identity") -- never a production
`financial_instrument_id` and never an authorization. Admission uses only
two positive bases: a modern same-session validated match, or one retained
legacy source-claimed ISIN; a record with neither is `BLOCKED_UNRESOLVED`,
and a record carrying both is an impossible shape that rejects its entire
session rather than guessing.

Two distinct listing lanes admissibly claiming the same ISIN in one session
are both `BLOCKED_SAME_SESSION_ISIN_COLLISION` -- no winner is ever
selected by order, liquidity, or symbol. Across sessions, the layer emits
past-only `LISTING_KEY_REBOUND` and `IDENTITY_SYMBOL_CHANGED` transitions
from bounded latest-observation state (keyed by listing key and by research
identity, never the whole corpus); a rebound never blocks the new identity
and never rewrites an earlier decision -- the identity key itself, not a
lookup-time check, is what prevents price continuity across a rebound.

`research_identity_admission_complete` grades only this admission step. It
never implies production identity resolution, corporate-action adjustment,
or any training/feature/label/alert/paper/execution authority -- those
remain separate, not-yet-built stages.

### Identity-bound raw price stream

`india_swing.evaluation.nse_archive_research_identity` also exposes
`iter_nse_archive_research_paired_sessions`, which pairs each replayed
session with its exact admission grade in one single pass over the archive
-- prices and identity decisions are never produced by two separate
traversals of the multi-year corpus. `iter_nse_archive_research_identity_admission_sessions`
now projects from this same paired stream, so it still performs exactly one
upstream replay traversal per call.

`india_swing.evaluation.nse_archive_research_price_stream` builds on the
paired stream: it binds every replayed EQ record to its exact identity
decision as one immutable observation per session, in replay-record order,
with raw OHLCV/delivery fields untouched -- never copied, recalculated, or
adjusted. Every record is retained, including `BLOCKED_UNRESOLVED` and
`BLOCKED_SAME_SESSION_ISIN_COLLISION` rows (with `research_identity_id`
left `None`); none are ever silently dropped. Admission transitions are
carried through byte-for-byte, exactly as the identity layer emitted them.

This stream is the lossless input a future cutoff-aware corporate-action
adjustment and feature stage will consume. It is not itself valid backtest
or model input: prices remain `RAW_UNADJUSTED`, `collection_only=True`, and
every actionable/training/feature/label/alert/execution flag stays false;
production identity resolution and corporate-action adjustment remain
false.

## Cross-session promoted history panel

`india_swing.historical_prices.promoted_history` assembles multiple verified
promoted-session tick snapshots against one exact collection calendar. It groups
only rows carrying the same stable instrument and listing identities, and it
creates one observation for every trading session between the first and last
supplied snapshots.

The panel is intentionally diagnostic:

- a missing whole-session snapshot, absent universe row, missing price bar, or
  identity conflict remains an explicit status rather than an inferred value;
- unresolved identities, source-excluded rows, and orphan bars remain separate
  retained evidence instead of disappearing from coverage;
- raw bar and observed tick-size lineage is preserved per session;
- prices remain `RAW_UNADJUSTED`; no interpolation, corporate-action
  adjustment, feature calculation, signal generation, alerting, or execution
  authority is introduced;
- the calendar, every input snapshot, every derived history, and the complete
  retained graph are replay-verified before the panel is accepted.

This panel is therefore the survivorship-safe raw-history bridge into a future
cutoff-aware corporate-action adjustment stage. It is not itself valid model or
backtest input.
