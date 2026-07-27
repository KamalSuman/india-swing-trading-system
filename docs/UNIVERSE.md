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

## Trusted promoted-identity session universe

`src/india_swing/universe/promoted_identity.py` implements
`PromotedIdentitySessionUniverseService`, a separate bridge from one
`VerifiedPromotedIdentityAdjudication` and one exact
`CollectionCalendarMaterialization` into a session-specific, small-cap-
inclusive NSE CM collection universe, without constructing the legacy
`CollectionUniverseSnapshot`, the promoted execution-facing
`reference.UniverseSnapshot`, or any market-cap/index/price/volume/
liquidity/model-score filter. Every parsed row of the exactly-selected
promoted security master receives exactly one
`PromotedIdentitySessionEntry` -- large and small issuers alike stay in
scope, and deleted, disappeared, or readmitted securities are always
retained verbatim (their raw `delete_flag`/timestamps are preserved; no
lifecycle-interval field is ever synthesized from them).

`materialize(adjudication, calendar, market_session, cutoff)` independently
replays both input content identities before deriving anything, requires
both inputs to remain `COLLECTION_ONLY`/non-actionable, requires the
calendar to be pinned to `NSE`/`CM`, requires `cutoff` to be at or after both
`adjudication.snapshot.knowledge_time` and `calendar.cutoff`, and requires
`market_session` to resolve to an executable trading session inside the
calendar's own coverage and day-kind graph -- holidays, weekends,
unscheduled closures, and out-of-coverage dates all fail closed. Exactly one
intake promotion whose `verified_report_date` equals `market_session` is
selected; zero or ambiguous matches fail closed, and there is no
latest/nearest/preceding/following selection.

For every `RETAINED_UNVERIFIED_EQUITY` source row, the service requires
exactly one matching `IdentityObservation`, exactly one candidate containing
it, and exactly one `CandidateIdentityResolution` from the retained
adjudication graph. A resolution with a `stable_instrument_id` additionally
requires exactly one `EffectiveStableListingObservation` whose stable
instrument ID, stable listing ID, symbol, series, ISIN, candidate ID, and
`effective_on` all agree exactly with that specific row's observation and
the selected `market_session` -- classified
`IDENTITY_RESOLVED_COLLECTION_ONLY`. A resolution with no
`stable_instrument_id` requires zero listing observations for that row --
classified `IDENTITY_UNRESOLVED`, retaining the complete required/missing
requirement and blocker-code sets. Excluded rows map exactly to their own
source exclusion disposition with no observation, candidate, resolution, or
stable-ID field at all. No entry is ever `ACTIONABLE` or `WATCH_ONLY`: the
resulting `VerifiedPromotedIdentitySessionUniverse` always keeps
`readiness=COLLECTION_ONLY`, `actionable=false`, and
`execution_eligible=false`, even when every retained row already has a
resolved stable identity.

`knowledge_time` is always `max(adjudication.snapshot.knowledge_time,
selected_promotion.knowledge_time, calendar.cutoff)` -- never the filesystem
or wall clock. `__post_init__` calls `verify_content_identity()`, which
requires the exact concrete type (rejecting subclasses/impostors even when
every retained field is otherwise valid) and independently replays the
complete derivation, so direct construction with a mismatched value or
post-construction mutation anywhere in the retained graph fails closed with
one static sanitized `PromotedIdentitySessionUniverseError`. This universe
remains evidence only: a structurally complete, fully-resolved session
universe is still not trading or alert authority on its own.
