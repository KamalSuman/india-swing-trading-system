# Exact promoted-input proposal preparation

`india-swing-proposal-prepare` is the restart-safe bridge from one stored
`SwingUniverseInputBatch` to the deterministic full-universe proposal graph.
It does not collect data, infer a latest snapshot, request Kite quotes, send a
notification, or place an order.

## Inputs

The parent store must already contain three exact content-addressed roots:

- a `SwingUniverseInputBatch` whose assemblies exactly cover every actionable
  subject and whose vetoes exactly cover every other scoped subject;
- the calendar snapshot bound by that universe; and
- the deterministic signal configuration.

Each input assembly already carries its stable instrument/listing identity,
ordered raw-price lineage, historical universes, corporate-action snapshot,
effective tick-size evidence, and replayed promotion decision. The preparation
spec additionally records one subject binding per assembly, including the
assembly and promotion-decision IDs. Removing or substituting one company
therefore changes the preparation and proposal identities.

`COLLECTION_ONLY` input batches are rejected. Synthetic fixtures can exercise
the research path but remain `research_only=true`. Point-in-time-verified inputs
can become non-research-only only when upstream official importers and
promotion evidence genuinely satisfy the existing model invariants.

## Scheduled command

The preferred invocation supplies only exact parent IDs:

```powershell
india-swing-proposal-prepare `
  --graph-root C:\absolute\restored-state\proposal_graph `
  --universe-batch-id <sha256> `
  --calendar-snapshot-id <sha256> `
  --signal-config-id <sha256>
```

The command loads only those IDs, deterministically derives the preparation
and expected proposal-batch IDs, stores the canonical preparation, recreates
every technical proposal, and publishes the terminal proposal manifest last.

An externally prepared canonical spec can instead be supplied with:

```powershell
india-swing-proposal-prepare `
  --graph-root C:\absolute\restored-state\proposal_graph `
  --spec-file C:\absolute\proposal-preparation.json
```

Mixing the two input modes is rejected. Neither mode enumerates parent files or
supports a latest alias.

## Replay and coverage guarantees

Before terminal publication, the bridge independently reconstructs a fresh
preparation spec from the stored parents. It verifies:

- exact universe, calendar, and policy IDs;
- signal session, cutoff, and readiness;
- stable subject ordering and uniqueness;
- every assembly and promotion-decision ID;
- scoped, proposal, and veto counts;
- research-only authority; and
- the deterministic expected proposal-batch ID.

The proposal manifest becomes visible only after the preparation and all three
parent roots are durable. A retry returns the same artifacts. Partial parent or
preparation writes do not create a schedulable proposal batch.

## Current real-data limitation

The current NSE archive remains collection-only. In particular, the reference
universe intentionally refuses point-in-time-verified construction until an
official eligibility/identity importer exists. This bridge preserves that
guardrail; it does not relabel collection data as promoted. The next required
data implementation is the official point-in-time identity, eligibility,
corporate-action, and tick provenance layer that can legitimately produce the
stored input batch consumed here.
