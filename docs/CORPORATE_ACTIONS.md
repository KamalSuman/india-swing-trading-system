# Point-in-time corporate-action ledger

Status: the event/snapshot contracts and a cutoff-specific adjusted-price view
are implemented. The official NSE row importer is not yet implemented because
no real official export sample has been supplied.

## Event contract

Every event binds:

- stable instrument and optional stable listing identity;
- action type and effective exchange session;
- claimed announcement time and local knowledge time;
- exact source artifact and row identity;
- normalized terms; and
- an optional superseded event for amendments or cancellations.

The first supported normalized terms are split/bonus pre- and post-action share
ratios and INR cash dividends. The mechanical raw-price factor for a confirmed
split or bonus is `pre_action_shares / post_action_shares`. A cash dividend does
not receive an automatic factor because it requires a contemporaneous reference
price and an explicit total-return methodology.

Rights issues, mergers, demergers, symbol/ISIN changes, and delistings are typed
events, but complex numeric terms are rejected until an action-specific contract
exists. This prevents a generic ratio from silently producing a wrong adjusted
series.

## Snapshot contract

A snapshot is sealed at a knowledge cutoff and includes only events known by
that cutoff. It preserves superseded events in lineage while exposing only the
latest confirmed event as active. Missing amendment targets, competing
amendments, future-known events, unknown source artifacts, and out-of-coverage
events fail closed.

Snapshots declare coverage, readiness, completeness, actionability, and exact
blocker codes. A helper converts the snapshot to the corporate-action capability
used by the promotion gate without upgrading any of those declarations.

## Adjusted-price view

`build_adjusted_price_history` consumes exact raw NSE bars, one point-in-time
stable-identity binding per bar, and one complete actionable corporate-action
snapshot. It creates a separate content-addressed view; raw artifacts are never
rewritten.

For confirmed splits and bonuses, every pre-effective bar receives the
mechanical price factor and reciprocal volume factor. The effective-session bar
is left untouched. Cancellations remove superseded actions. Cash dividends,
rights, mergers, demergers, and other complex actions fail closed because no
safe automatic total-return factor has been defined.

The signal-history adapter binds the resulting bars, effective tick size, stable
identity, and corporate-action snapshot into the deterministic swing engine's
evidence contract. Collection-only, future-known, mismatched-identity, or
wrong-listing inputs are rejected.

## Promoted-history bridge

`corporate_actions.promoted_adjustments` connects the cross-session promoted
stable-listing panel to the same automatic split/bonus factor policy without
converting reconciled corpus bars into richer NSE report bars. That distinction
is important: fields absent from the provider corpus, such as traded value, are
never fabricated.

For each resolved stable listing, the bridge:

- binds every observed corpus bar to the exact session-universe content ID;
- applies only split/bonus evidence known by the requested cutoff;
- produces a separate adjusted OHLCV corpus view while preserving the original
  provider, request, reconciliation, raw-bar, identity, and action lineage;
- blocks histories containing gaps or identity conflicts;
- blocks incomplete/non-actionable action coverage; and
- routes dividends and other unsupported actions to explicit manual review.

Blocked histories remain in the output rather than being silently excluded.
Even a fully adjusted bridge remains `COLLECTION_ONLY` and feature-ineligible:
the collected security-master tick value is not yet a verified effective-dated
tick interval.

## Durable snapshot store

`src/india_swing/corporate_actions/snapshot_store.py` implements
`LocalCorporateActionSnapshotStore`, the durable root store that lets a
`CorporateActionSnapshot` survive a process restart. `CorporateActionSnapshot`
has no lower durable source, so this is a leaf persistence boundary only: it
neither upgrades nor downgrades whatever valid `readiness`/`complete`/
`actionable`/reason-code state the source snapshot already carries, and a
successfully stored snapshot is never independent official-source
provenance -- it proves only that the exact content was durably persisted.
`get(snapshot_id)` never trusts a stored ID: it strictly decodes every
field (rejecting duplicate keys, float/NaN/Infinity tokens, unknown/missing
fields, malformed hashes/dates/aware-UTC datetimes/enums, bool-as-int, and
noncanonical or non-positive Decimal economic terms) and constructs genuine
`CorporateActionEvent`/`CorporateActionSnapshot` values, so their own
construction-time validation and content identity independently replay.
Decimal economic terms are validated by exact canonical-text round-trip
(`str(Decimal(value)) == value`), not by a restrictive digit-only shape: this
accepts every strictly positive representation `Decimal` can itself produce,
including scientific-notation forms such as `"1E+2"` or `"1E-7"`, and rejects
only non-finite, zero, negative (including every signed-zero spelling), or
non-round-tripping text -- never using `float` anywhere in the check.
`put(value)` decodes its own freshly encoded payload and requires exact
reconstructed equality (including `snapshot_id`) before writing anything, so
a value the codec cannot faithfully replay leaves no target artifact behind.
Exposes only `put`, `get`, and `path_for`; there is no list, latest, or
nearest-selection capability.

## Remaining work

Once a real official NSE export is available, the next increment will add:

1. byte-exact manual import and immutable raw storage;
2. pinned header/row parsing for the observed NSE schema;
3. explicit publication and amendment timing rules;
4. stable-identity resolution for every affected security;
5. regression fixtures taken from sanitized real row shapes; and
6. validated cash-dividend total-return methodology if dividends are included
   in strategy features.

Raw historical-price artifacts will never be rewritten. Adjustment views must
be separately versioned by their knowledge cutoff so a future split, dividend,
or amendment cannot change an earlier research decision.
