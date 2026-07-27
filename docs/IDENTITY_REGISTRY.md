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

## Trusted multi-vintage promotion intake

`src/india_swing/identity_registry/promoted_intake.py` implements
`PromotedIdentityIntakeService`, a separate bridge from multiple already
independently verified `VerifiedReferenceArtifactPromotion` values into this
same candidate/adjudication machinery, without ever constructing a
`CrossVintageIdentityRegistry`. It reuses the exact same internal
conflict/candidate/transition graph builder and adjudication case builder the
legacy `materialize_cross_vintage_identity_registry`/
`build_identity_adjudication_queue` functions call, so promoted and legacy
inputs can never diverge in policy.

`PromotedIdentityIntakeService.materialize(promotions, expected_report_dates,
cutoff)` requires at least two distinct `expected_report_dates`, independently
replays every promotion's own content identity, rejects duplicate promotion/
join/source-artifact/manifest lineage and duplicate verified report dates,
requires the exact promoted report-date set to equal `expected_report_dates`,
and requires every promotion's `knowledge_time` to be at or before `cutoff` --
never inferring knowledge time from a manifest's `validated_at`,
`first_seen_at`, or any local clock. One `IdentityObservation` is built for
every `RETAINED_UNVERIFIED_EQUITY` row in every promoted artifact, binding
`claimed_report_date`/`knowledge_time` to the promotion's own trusted
`verified_report_date`/`knowledge_time` -- the manifest's own
`claimed_report_date`/`validated_at` never enter an observation this way,
unlike the legacy collection-only path.

For every resulting adjudication-queue case, exactly one
`IdentityRequirementSatisfaction` records that trusted acquisition/promotion
provenance already satisfies exactly `AUTHORIZED_SOURCE_PROVENANCE` and
`REPORT_DATE_VERIFICATION`; every other policy requirement (adjacent-vintage
observation, validated identifier, official continuity/lifecycle/
listing-status/conflict-resolution evidence) remains explicitly unresolved.
The resulting `VerifiedPromotedIdentityIntake` retains `source_readiness=
POINT_IN_TIME_VERIFIED` (describing only the retained source promotions)
while its own `readiness` stays `COLLECTION_ONLY`, `actionable` stays
`false`, and `stable_identity_assigned` stays `false` -- satisfying two
requirements per candidate is not stable-identity adjudication, and this type
exposes no `StableInstrumentId`, `StableListingId`, universe, calendar,
price, liquidity, corporate-action, model, signal, or trading-authority
field. `__post_init__` calls `verify_content_identity()`, which requires the
exact concrete type and independently replays the complete derivation, so
direct construction with a mismatched value or post-construction mutation
anywhere in the retained promotion/observation/candidate/transition/conflict/
queue graph fails closed with one static sanitized
`PromotedIdentityIntakeError`. It never rewrites, replaces, or otherwise
mutates any sealed source archive, and it creates no second archive,
persistence format, or CLI of its own in this boundary.

## What completes this boundary

Recurring authorized master collection must supply multiple consecutive
vintages. Official listing, suspension, delisting, merger/demerger, rename, and
corporate-action evidence must then satisfy the implemented queue and adjudicate
candidates into effective-dated instrument and listing identities. Evidence
import and adjudication decisions are not implemented yet. Promotion remains
impossible until that evidence has verified acquisition and publication-time
provenance.
