# Promoted research-to-operational-preparation bridge

`src/india_swing/promoted_operational_preparation.py` is the first seam
between one exact, already-published `PromotedResearchRunManifest` and a
durable, restart-safe, **paper-only** operational-preparation boundary. It
retains the exact `PromotedResearchTradeIntent` objects a promoted research
run's selected candidates produced, together with their complete promoted
lineage, and derives only a canonical NSE listing key and a target session
for each -- nothing else. It creates no new financial algorithm, selects or
reranks no candidate, and never acquires a quote, sends a notification, or
touches a broker.

## Exact-ID contract and the SwingProposalBatch boundary

The existing Swing operational stack (`signals/proposal_batch.py`,
`operations/models.py`) requires an exact `SwingProposalBatch`/
`SwingTechnicalProposal` graph -- the exact `SwingInputAssembly`,
`UniverseEntry`, `CalendarSnapshot`, metrics, and configuration that graph
needs. A `PromotedResearchTradeIntent` is a different domain object and does
not carry those exact types. This module therefore does **not** fabricate or
partially populate a `SwingProposalBatch`: it publishes an honest,
lower-level preparation boundary instead, and a later task will adapt the
quote/risk/decision machinery around a shared operational-candidate
contract.

`PromotedOperationalCandidate` retains the exact `PromotedResearchTradeIntent`
plus the source `research_run_id`/`research_intent_batch_id`, and derives
only:

- `listing_key` -- exactly `NSE:<evaluation_intent.entry_order.symbol>`.
- `target_session` -- the entry order's own single-day
  `first_eligible_session`, which must already equal its `expiry_session`.

No price range, probability, confidence, ATR, calendar window, or new
quantity/stop/target is invented; every such value remains authoritative
only inside the retained `research_intent`.

## Publication and replay boundary

`prepare_and_publish(research_run_id, research_stores, preparations)` is the
only entry point that starts from a bare `research_run_id`: it resolves the
exact research run, derives and resolves its exact `engine_run_id`, derives
and resolves that run's exact `research_intent_batch_id` -- never a
different or "latest" run -- and calls the pure
`PromotedOperationalPreparationService`, which independently re-verifies all
three resolved objects, cross-checks every session/cutoff/count/readiness/
authority field between them, and deterministically reconstructs the exact
candidate tuple in the research batch's own canonical intent order (never
re-ranked by symbol, never silently deduplicated). A zero-selected-intent
batch is prepared as an explicit, auditable `selected_count=0` preparation,
not an exception.

`LocalPromotedOperationalPreparationStore.get` never trusts the stored
manifest as authority: it strictly decodes it, then independently resolves
the exact research run, derives and resolves its exact engine run, derives
and resolves that run's exact research-intent batch, reruns the preparation
service, and requires the reconstructed canonical manifest bytes and
`preparation_id` to match the stored artifact exactly. `put` performs this
same full re-verification before ever writing. Both `prepare_and_publish`
and the store share one operation-scoped `ExactReplayScope`
(`build_promoted_operational_preparation_store` reuses
`build_promoted_research_stores`'s own already-constructed scope unchanged
-- it never builds a second promoted graph or a second, unrelated scope),
so within one top-level operation each exact ancestor is resolved at most
once, while a later call or process restart always performs one final cold
replay outside that scope.

## Safe retry semantics

The preparation store is the same create-once, content-addressed store
pattern already used elsewhere: a repeated `put` of the identical
preparation is idempotent, and conflicting bytes at the same content-derived
path fail closed. It exposes only `put`, `get`, and `path_for` -- no list/
latest/nearest/find/discovery operation.

## CLI shape

```
india-swing-promoted-operational-prepare \
  --reference-root <path> --identity-evidence-root <path> \
  --calendar-root <path> --daily-reports-root <path> \
  --historical-corpus-root <path> --promoted-root <path> \
  --graph-publication-root <path> --engine-run-root <path> \
  --research-run-root <path> --operational-preparation-root <path> \
  --research-run-id <sha256>
```

There is no flag for the engine run or research-intent batch: both are
always derived from `--research-run-id`. Success prints one sanitized JSON
object with the combined `preparation_id`, every retained lineage ID
(research/graph/engine/batch), the sessions/cutoff, the ordered
`candidate_ids`/`research_intent_ids`/`listing_keys`, `selected_count`/
`blocked_count`/`source_universe_complete`, the carried-through `readiness`,
and `paper_only: true`/`notification_eligible: false`/
`execution_eligible: false`. A zero-candidate preparation is a normal,
successful, auditable result.

Every parse failure (missing required argument, unknown option, malformed
value) is caught by the same `SanitizedArgumentParser` pattern already used
by every other promoted CLI in this project: it raises instead of printing
argparse's usage text, so it flows through the same boundary as an ordinary
runtime failure -- exit code 2 with only
`{"status": "FAILED", "error_type": "..."}`, never a raw argument value,
path, ID, or parser message. `--help` is unaffected.

## Paper-only meaning and explicit non-goals

Every published preparation has `paper_only: true`,
`notification_eligible: false`, and `execution_eligible: false`
permanently, and its `readiness` is carried through from the source
research batch exactly (currently always `COLLECTION_ONLY`) -- never
upgraded. This module does not acquire a quote, send a Telegram alert,
touch a broker, GCP, the network, or a live store, does not create or
coerce a `SwingProposalBatch`/`SwingTechnicalProposal`, does not select or
rerank a candidate, and does not change any risk/quantity/entry/stop/
target/tick/cost-buffer/holding-period value retained inside a research
intent. Quote acquisition and decision execution remain the next milestone.
