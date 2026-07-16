# Point-in-time corporate-action ledger

Status: the event and snapshot contracts are implemented. The official NSE
row importer and adjusted-price view are not yet implemented because no real
official export sample has been supplied.

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

## Remaining work

Once a real official NSE export is available, the next increment will add:

1. byte-exact manual import and immutable raw storage;
2. pinned header/row parsing for the observed NSE schema;
3. explicit publication and amendment timing rules;
4. stable-identity resolution for every affected security;
5. point-in-time adjustment views derived from immutable raw bars; and
6. regression fixtures taken from sanitized real row shapes.

Raw historical-price artifacts will never be rewritten. Adjustment views must
be separately versioned by their knowledge cutoff so a future split, dividend,
or amendment cannot change an earlier research decision.
