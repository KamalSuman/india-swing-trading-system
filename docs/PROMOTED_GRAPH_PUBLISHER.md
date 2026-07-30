# Promoted-graph publisher

`src/india_swing/promoted_graph_publisher.py` is the one restart-safe,
exact-ID publisher that composes the existing promoted services -- identity
intake, identity adjudication, per-session identity universe, per-session
market-data frame, per-session tick snapshot, stable-listing history,
corporate-action adjustment, and effective-session ticks -- from already
stored evidence, ending in the exact `adjustment_bridge_id` and
`effective_tick_panel_id` roots the existing `PromotedEngineRunner` already
consumes. It creates no new financial algorithm: every step is delegated
unchanged to its existing service and durable store from
`promoted_graph_store.py`.

## Exact-ID contract

A `PromotedGraphPublicationSpec` pins every source by its exact content ID,
never by discovery:

- `promotion_bindings` -- a sorted, unique, non-empty tuple of
  `(promotion_id, expected_report_date)` pairs. A duplicate date or a
  duplicate promotion ID is rejected rather than silently deduplicated.
- `identity_evidence_artifact_ids` / `identity_review_bundle_ids` -- sorted,
  unique tuples of exact IDs. Empty is allowed: the existing adjudication
  service itself decides whether the supplied evidence/review is sufficient
  to resolve a stable identity, and an unresolved candidate is preserved as
  an explicit `IDENTITY_UNRESOLVED` entry rather than dropped.
- `calendar_materialization_id` -- the exact sealed calendar to use for
  every session.
- `session_bindings` -- a sorted, unique, non-empty tuple of
  `(market_session, historical_corpus_id)` pairs. Multiple sessions may
  reference the same corpus ID (a corpus can hold more than one session's
  partition); the publisher selects, for each binding, exactly the one
  corpus partition whose own `market_session` matches -- zero or more than
  one match fails closed.
- `corporate_action_snapshot_id` -- the exact corporate-action snapshot to
  adjust the assembled history against.
- `cutoff` -- one aware-UTC instant used for every step. Every downstream
  service already enforces its own cutoff-after-knowledge-time checks, so
  a single sufficiently-late cutoff naturally satisfies the entire chain.

`spec_id` is a canonical content hash over every field above. The publisher
never lists, scans, selects a latest artifact, chooses a nearest date, falls
back to a default, fetches network data, or synthesizes missing evidence:
every resolution is `store.get(exact_id)`.

## Publication and replay boundary

`PromotedGraphPublisher.publish` walks the chain in the architecture's fixed
order, and after every single materialization it **persists the artifact
and reads it back** before proceeding to the next step. A terminal
`PromotedGraphPublicationManifest` is constructed and published only after
every intermediate and terminal artifact (identity intake, adjudication,
each session's universe/frame/tick-snapshot, the stable-listing history, the
corporate-action adjustment, and the effective-session ticks) is durable and
independently re-read. If any step fails, no terminal manifest is ever
constructed; whatever content-addressed intermediates already exist remain
as safe, idempotent artifacts and are reused (not rebuilt) on a retry with
the same spec.

The manifest does not persist an opaque `spec_id` alone: it retains the
**complete spec preimage** (every root pin above) and, on every
construction, decode, and verification, recomputes `spec_id` from that
retained preimage via the same shared function `PromotedGraphPublicationSpec`
itself uses -- a manifest whose retained fields do not recompute to its own
`spec_id` fails closed. `LocalPromotedGraphPublicationStore.get` goes
further: it independently re-resolves the identity intake, adjudication,
calendar, every per-session universe/frame/tick-snapshot, the stable-listing
history, the corporate-action adjustment, and the effective-session ticks
through their own real durable stores, and requires each of them to agree
with the manifest's own retained root pins (exact promotion IDs and
expected report dates reachable through the intake, the exact evidence/
review IDs reachable through the adjudication, the exact session/corpus
binding reachable through each frame, and so on) before recomputing
`manifest_id` and requiring canonical-byte equality. A manifest that is
fully self-consistent on its own terms (its own `spec_id`/`manifest_id`
correctly recompute from its own retained, tampered fields) still fails
closed if those retained root pins disagree with what the independently
reconstructed downstream lineage actually resolves to.

`build_promoted_graph_stores` is the one production composition function:
given seven explicit roots (reference data, identity evidence, calendar
data, daily reports, historical corpus, promoted evidence, and publication
evidence) it constructs every real store this project already has and wires
every nested resolver explicitly -- there is no hidden default and no list/
latest/nearest/find capability anywhere in the composition.

Resolver replay is deduplicated only inside one top-level publication or
manifest read. The operation-scoped cache is keyed by exact store namespace
and content ID, and is cleared when that operation exits. Each promoted
store's `put` still performs its existing pre-write replay and post-write
fresh read; the publisher does not add a redundant third read. After graph
construction, terminal publication is followed by a new cold manifest read
with an empty cache. A later call or process restart therefore reopens and
verifies the durable graph rather than inheriting trust from an earlier run.
The manifest verifier loads both terminal roots independently and then
checks every retained intermediate/root pin against their fully
reconstructed nested lineage, avoiding repeated reconstruction of the same
immutable ancestor without removing any lineage gate.

## Safe retry semantics

Every intermediate store is the same create-once, content-addressed store
already used elsewhere in this project: a repeated `put` of the identical
artifact is idempotent, and conflicting bytes at the same content-derived
path fail closed. Because the publisher persists and reads back after every
step, re-running `publish` with the same spec after a partial failure simply
resumes from whichever intermediates already exist -- it never re-derives an
artifact that is already durable, and it never leaves a corrupted or
half-written terminal manifest.

## CLI shape

```
india-swing-promoted-graph-publish \
  --reference-root <path> --identity-evidence-root <path> \
  --calendar-root <path> --daily-reports-root <path> \
  --historical-corpus-root <path> --promoted-root <path> \
  --publication-root <path> \
  --promotion-binding <sha256>@<YYYY-MM-DD> [--promotion-binding ... repeatable] \
  [--identity-evidence-id <sha256> ...] [--identity-review-id <sha256> ...] \
  --calendar-materialization-id <sha256> \
  --session-binding <YYYY-MM-DD>@<sha256> [--session-binding ... repeatable] \
  --corporate-action-snapshot-id <sha256> \
  --cutoff <aware-ISO-8601-datetime>
```

Success prints one sanitized JSON object: `status`, `spec_id`,
`manifest_id`, every retained root pin, `intake_id`, `adjudication_id`,
the ordered per-session `session_artifacts`, `stable_history_panel_id`,
`adjustment_bridge_id`, `effective_tick_panel_id`, the terminal readiness/
actionability projections, `paper_only: true`, and `execution_eligible:
false`. A resolved-but-blocked/incomplete downstream state is a normal,
successful, auditable publication -- not an exception.

Every parse failure (missing required argument, unknown option, malformed
`--promotion-binding`/`--session-binding` syntax) is caught by a
`SanitizedArgumentParser` that raises instead of printing argparse's usage
text, so it flows through the same boundary as an ordinary runtime failure:
exit code 2 with only `{"status": "FAILED", "error_type": "..."}` -- never a
raw argument value, path, ID, or parser message. `--help` is unaffected.

## Paper-only meaning and explicit non-goals

`paper_only` is always `true` and `execution_eligible` is always `false` on
every published manifest, regardless of the resolved readiness/
actionability of the underlying graph: publication proves only that a
deterministic graph was durably assembled from exact evidence, never that
any downstream research signal is authorized for a live decision.

This module deliberately stops at the two exact roots
`PromotedEngineRunner` already consumes. It does not run the promoted
engine, prepare a proposal, generate a research intent, send an alert, or
touch a broker, Telegram, GCP, the network, or a live store. It never
creates, imports, adjudicates, or promotes upstream reference-artifact or
identity evidence -- it only consumes exact, already-stored evidence roots.
