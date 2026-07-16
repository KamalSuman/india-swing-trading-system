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

## Complete adjudication queue

`build_identity_adjudication_queue` converts every candidate—not a selected
subset—into one immutable evidence case. Every case requires authorized source
provenance and verification of the claimed report date. Additional requirements
are derived rather than caller supplied:

- single-vintage rows require an adjacent vintage and official listing status;
- invalid identifiers require a validated identifier and another vintage;
- multi-vintage ISIN candidates require official continuity confirmation;
- symbol, series, financial-ID, or name changes require official lifecycle evidence;
- identifier, ticker, or financial-ID conflicts require official conflict resolution;
- any `DelFlg=Y` observation requires official listing-status evidence because
  the flag alone is never interpreted as a delisting.

The queue remains `COLLECTION_ONLY`, `actionable=false`, and
`stable_identity_assigned=false`. It is a complete work list, not an
adjudication outcome. `LocalIdentityAdjudicationQueueStore` publishes one
create-once queue per sealed registry and replays the registry and all underlying
security-master bytes on every read. Tampered, partial, extra, or selectively
regenerated queues fail closed.

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

Create and inspect the evidence queue only after the candidate registry is
persisted:

```powershell
python -m india_swing.identity_registry.cli adjudication-materialize `
  --registry-id <sealed-registry-id>

python -m india_swing.identity_registry.cli adjudication-show `
  --registry-id <sealed-registry-id>

python -m india_swing.identity_registry.cli adjudication-list
```

The summaries contain requirement counts, never a fabricated stable identity.
The current 21,133-observation local registry takes roughly one minute to replay
from raw sealed masters on this machine. Publication avoids a redundant second
replay, while every independent read still performs one full replay.

## What completes this boundary

Recurring authorized master collection must supply multiple consecutive
vintages. Official listing, suspension, delisting, merger/demerger, rename, and
corporate-action evidence must then satisfy the implemented queue and adjudicate
candidates into effective-dated instrument and listing identities. Evidence
import and adjudication decisions are not implemented yet. Promotion remains
impossible until that evidence has verified acquisition and publication-time
provenance.
