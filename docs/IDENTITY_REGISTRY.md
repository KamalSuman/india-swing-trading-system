# Cross-vintage identity registry

This layer compares sealed NSE CM MII security-master vintages without
pretending that a ticker, ISIN, or NSE financial-instrument number is already a
permanent company identity. Its output is research evidence only:
`COLLECTION_ONLY`, `actionable=false`, and `stable_identity_assigned=false`.

## Why this exists

A survivorship-safe history must preserve companies that later disappear and
must distinguish symbol changes from ticker reuse. A current instrument dump
cannot reconstruct either fact. Every dated master therefore contributes a
set of positive observations known at its own validation time.

The registry deliberately follows these rules:

- one retained master row becomes exactly one immutable observation;
- a validated ISIN may create a continuity *candidate* across vintages;
- an invalid or unvalidated source identifier is isolated to one observation;
- concurrent rows for one ISIN in different NSE series are preserved as
  separate listing observations, not mislabeled as conflicts;
- more than one row for the same ISIN, date, and series is a conflict;
- reuse of a financial-instrument number or `symbol + series` across different
  identifiers is a conflict, never an automatic join;
- absence from a later master is not called a delisting, suspension, or removal;
- listing transitions are emitted only when adjacent-vintage rows can be paired
  uniquely by listing key, financial ID, series, or a final one-to-one match;
- ambiguous listing lanes remain unlinked and conflicting candidates produce
  no transitions;
- no candidate is promoted to the stable instrument/listing IDs consumed by
  the decision pipeline.

The last two restrictions are essential. Positive daily snapshots can show
that a row existed, but they cannot prove why a row disappeared or that two
legal/economic entities are identical after a merger, demerger, relisting, or
identifier correction. Those decisions require separately audited lifecycle
evidence.

## Cutoff and replay contract

Every source artifact must already exist in the sealed reference-data store,
must pass raw-byte provenance replay, and must have `validated_at <= cutoff`.
Only one source is accepted for each claimed report date. The claimed date is
still explicitly unverified when the source came from a manual portal
download.

The registry store is create-once and content addressed. On both write and
read it reloads every source from the reference store, reparses the original
gzip, rematerializes the registry, and compares the exact payload and IDs. An
unexpected file, link, altered manifest, altered payload, or changed source
causes a fail-closed read.

## Command

Run from the repository root after setting `PYTHONPATH=src` if the package is
not installed in editable mode:

```powershell
python -m india_swing.identity_registry.cli materialize `
  --security-master-id <artifact-id-for-date-1> `
  --security-master-id <artifact-id-for-date-2> `
  --cutoff 2026-07-16T18:00:00+05:30
```

Repeat `--security-master-id` in chronological coverage order or any other
order; the materializer deterministically sorts by claimed report date. The
CLI prints observation, candidate, transition, and conflict counts and seals
the result under `INDIA_SWING_IDENTITY_REGISTRY_ROOT` (default
`var/identity_registry`).

## What completes this boundary

Recurring authorized master collection must supply multiple consecutive
vintages. Official listing, suspension, delisting, merger/demerger, rename, and
corporate-action evidence must then adjudicate candidates into effective-dated
instrument and listing identities. Promotion remains impossible until that
evidence has verified acquisition and publication-time provenance.
