# Broad collection universe

Status: the source-backed collection materializer, replay-verified local store,
sanitized CLI, and promotion adapter are implemented. No real universe snapshot
is research-eligible, backtest-eligible, or alert-eligible.

## Scope

The collection policy applies no market-cap cutoff. It processes every row in
one explicitly selected sealed NSE CM security master and records exactly one
observation per source row. The broad equity subset is only the parser's
`RETAINED_UNVERIFIED_EQUITY` disposition. Non-equities, test securities, and
alternative-venue records retain explicit exclusion dispositions.

This design prevents smaller main-board companies from disappearing merely
because they are outside a large-cap index. It does not imply that every
retained row is safe or tradable.

## Facts retained

Each observation preserves the source record and manifest lineage, exchange
instrument ID, symbol, series, validated ISIN when available, raw
permitted-to-trade value, normal-market status and eligibility, delete flag,
and listing/removal/readmission timestamps.

Those raw fields are not sufficient to prove an active, unsuspended,
main-board, surveillance-free listing at a historical cutoff. The collection
snapshot therefore never creates temporary stable IDs and never constructs the
promoted `reference.UniverseSnapshot` model.

## Mandatory blockers

Every v1 collection snapshot remains non-actionable and records:

- unverified board classification;
- unverified calendar provenance;
- unverified point-in-time listing state;
- unavailable stable identity;
- unavailable surveillance state;
- unverified manual acquisition; and
- unverified report date.

Passing these blockers later requires independently reviewed evidence. Merely
observing the same ISIN or symbol in another file is not enough.

## CLI

```powershell
python -m india_swing.universe.cli materialize `
  --security-master-id <sealed-artifact-id> `
  --calendar-snapshot-id <explicit-calendar-snapshot-id> `
  --cutoff <ISO-8601-cutoff>
```

Use `show --snapshot-id <id>` and `list` for replay-verified inspection. Set
`INDIA_SWING_UNIVERSE_ROOT` to override the default `var/universe` store.

## Current real diagnostic

Snapshot `f9dca3a8233f2249aee8455032c080cb670f8f1376cdd2fc747ecde3fdf05b48`
was materialized for the 16 July 2026 session claim. It contains:

- 36,062 audited source rows;
- 21,133 in-scope unverified equity rows;
- 14,906 excluded non-equities;
- 23 excluded test securities; and
- no market-cap cutoff.

This is the broad candidate intake requested for small-cap coverage, but it is
not yet a tradable candidate set. Stable identities, point-in-time listing
status, board classification, surveillance, and adequate trailing liquidity
must still be joined before research or alerts.
