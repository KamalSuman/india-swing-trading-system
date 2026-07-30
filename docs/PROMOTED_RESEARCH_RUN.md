# Promoted graph-to-engine research bridge

`src/india_swing/promoted_research_run.py` is the one restart-safe,
exact-manifest bridge from one already-published `PromotedGraphPublicationManifest`
into the existing `PromotedEngineRunner`. It derives every engine root pin
from the resolved graph manifest, runs the existing engine unchanged, and
durably binds both terminal IDs into one combined
`PromotedResearchRunManifest`. It creates no new financial algorithm and
republishes nothing: the upstream graph and the engine are both consumed
exactly as they already exist.

## Trust boundary

`PromotedResearchRunRequest` pins exactly one `graph_manifest_id` plus the
run's own `signal_session`, `entry_session`, aware-UTC `cutoff`,
`initial_capital`, and the three existing engine config types. It
deliberately has **no field** for `adjustment_bridge_id`,
`effective_tick_panel_id`, the reference-promotion set, or the
corporate-action snapshot ID: those are always *derived* from the resolved
graph manifest inside `PromotedResearchOrchestrator.run`, never accepted or
overridden separately. A caller cannot smuggle in a different engine root
than the one the published graph actually names.

`PromotedResearchOrchestrator.run` resolves `request.graph_manifest_id`
through the exact same `LocalPromotedGraphPublicationStore` the graph
publisher itself uses, and requires the resolved manifest's own
`paper_only`/`execution_eligible` fields to be `true`/`false`. A missing,
malformed, or substituted graph fails closed with no engine call and no
combined manifest. The graph's own `adjustment_readiness`/
`adjustment_actionable`/`effective_tick_readiness`/
`effective_tick_actionable` are **not** a gate here: the identity-
adjudication service every promoted graph is built from is permanently
`COLLECTION_ONLY`/non-actionable by its own explicit contract, so requiring
actionability would make every real graph unusable for paper research. This
bridge instead runs the existing engine for a collection-only graph exactly
as it already supports, and carries the graph's own readiness/actionable
projections through onto the combined manifest unchanged -- preserved
evidence, never upgraded, and never treated as broker/execution authority.

Everything after that boundary is the existing, unmodified engine: the
bridge constructs one `PromotedEngineRunRequest` from the derived pins plus
the request's own sessions/cutoff/capital/configs and calls
`PromotedEngineRunner.run` exactly as `promoted_engine_cli.py` already does.
Every existing engine check (`entry_session > signal_session`, the resolved
adjustment panel's own `signal_session` match, cutoff-after-knowledge,
corporate-action/promotion-set agreement) still applies unchanged; this
bridge invents no additional trading-calendar, sizing, cost, liquidity,
stop, target, or exposure policy.

## Derived-root design

| Derived engine pin | Source on the graph manifest |
| --- | --- |
| `adjustment_bridge_id` | `graph_manifest.adjustment_bridge_id` |
| `effective_tick_panel_id` | `graph_manifest.effective_tick_panel_id` |
| `expected_reference_promotion_ids` | sorted `promotion_id` from `graph_manifest.promotion_bindings` |
| `expected_corporate_action_snapshot_id` | `graph_manifest.corporate_action_snapshot_id` |

`PromotedResearchRunManifest` retains the complete research-request
preimage (`graph_manifest_id`, sessions, cutoff, capital, and all three
config IDs) alongside every one of these derived pins, every engine output
ID (`engine_request_id`, `engine_run_id`, `feature_input_panel_id`,
`technical_panel_id`, `cross_section_panel_id`, `research_intent_batch_id`,
`replay_run_id`, `candidate_count`, `intent_count`), and the graph's own
`adjustment_readiness`/`adjustment_actionable`/`effective_tick_readiness`/
`effective_tick_actionable` projections. On every construction, decode, and
verification it recomputes `research_request_id` from that retained
preimage via the same shared function `PromotedResearchRunRequest` itself
uses -- exactly the pattern already applied to
`PromotedGraphPublicationManifest`'s `spec_id` and
`PromotedEngineRunManifest`'s `request_id` -- so a manifest whose retained
fields do not recompute to its own `research_request_id` fails closed.

## Replay and idempotency boundary

`LocalPromotedResearchRunStore.get` never trusts the stored manifest as
authority. It strictly decodes it, then resolves the graph publication and
the engine run **exactly once each** through their own already-hardened
durable stores -- each of which independently replays its own complete
graph on every call -- and requires every retained relationship between
them and the combined manifest to agree (exact root pins, exact engine
output IDs, exact sessions/cutoff/capital/counts, the graph's exact
readiness/actionable projections, and every paper-only/eligibility flag).
It never re-walks the deeper identity/session graph itself a third time:
that full replay already happened inside the two terminal `get` calls. A
manifest that is fully self-consistent on its own terms (its own
`research_request_id`/`research_run_id` correctly recompute from its own
retained, tampered fields) still fails closed if those retained pins
disagree with what the independently reconstructed graph or engine
manifest actually resolves to.

Every store here is the same create-once, content-addressed store already
used elsewhere: a repeated `put` of the identical manifest is idempotent,
and conflicting bytes at the same content-derived path fail closed.

## Shared replay scope and performance boundary

`src/india_swing/_exact_replay.py` holds `ExactReplayScope`/
`ScopedExactResolver`, generalized from the mechanism first built for the
graph publisher (`promoted_graph_publisher.py` keeps `_ReplayScope`/
`_ScopedExactResolver` as private aliases of the same classes, so its own
behavior and tests are unaffected). `build_promoted_engine_stores` now wires
every one of its own nested resolvers through one such scope too, via the
new bounded helper `build_promoted_engine_downstream_stores` -- it builds
only the engine's downstream stores (feature inputs, technical features,
cross-sections, research intents, engine runs) from caller-supplied exact
`corporate_action_adjustments`/`effective_session_ticks` resolvers, a
`promoted_root`, an `engine_run_root`, and an `ExactReplayScope`. The full
seven-root `build_promoted_engine_stores` factory builds its own upstream
identity/session/history chain and then delegates to this same helper, so
both entry points share one wiring.

`build_promoted_research_stores` builds the promoted graph exactly once via
`build_promoted_graph_stores`, then calls
`build_promoted_engine_downstream_stores` directly against that graph's own
`corporate_action_adjustments`/`effective_session_ticks` stores and its own
`ExactReplayScope` (`graph_stores._replay_scope`) -- it never constructs a
second, independent upstream identity/session/history resolver graph for
the engine side. `PromotedResearchOrchestrator.run` and
`LocalPromotedResearchRunStore.get` both open that same shared scope
(`stores._replay_scope`/`self._replay_scope`) around their own work, exactly
mirroring `PromotedGraphPublisher.publish`'s own
`with stores._replay_scope.open(): ...` pattern: every exact ancestor
(promotion, evidence, review, intake, adjudication, calendar, corpus,
frame, tick snapshot, history, corporate-action snapshot, adjustment,
effective ticks) is resolved **at most once** within one top-level
`PromotedResearchOrchestrator.run`/`LocalPromotedResearchRunStore.get`
call, no matter how many different stores or layers ask for it. Both public
entry points still return one **final cold get** performed after their own
scope has closed, so a fresh, uncached replay always happens at least once
per top-level operation -- caching only ever removes *duplicate* work
inside that one operation, never across separate calls or a process
restart. This is verified directly with counting resolver wrappers in the
test suite, which assert each exact ancestor is resolved exactly once per
operation and that a second, separate top-level call performs fresh
resolver calls again.

## CLI shape

```
india-swing-promoted-research-run \
  --reference-root <path> --identity-evidence-root <path> \
  --calendar-root <path> --daily-reports-root <path> \
  --historical-corpus-root <path> --promoted-root <path> \
  --graph-publication-root <path> --engine-run-root <path> \
  --research-run-root <path> \
  --graph-manifest-id <sha256> \
  --signal-session <YYYY-MM-DD> --entry-session <YYYY-MM-DD> \
  --cutoff <aware-ISO-8601-datetime> --initial-capital <decimal>
```

There is no flag for any engine root pin: they are always derived from
`--graph-manifest-id`. The three engine configs always use their existing
default constructors; success output includes their exact `config_id`s so
the caller can audit which configuration produced the result. Success
prints one sanitized JSON object with the combined `research_run_id`/
`research_request_id`, every graph root pin, every engine output ID, the
sessions/cutoff/capital/counts, the resolved graph's own
`adjustment_readiness`/`adjustment_actionable`/`effective_tick_readiness`/
`effective_tick_actionable` (preserved exactly, never upgraded), and
`paper_only: true`, `notification_eligible: false`,
`execution_eligible: false`. A collection-only/not-yet-actionable graph is a
normal, successful, auditable paper-research result -- not an exception.

Every parse failure (missing required argument, unknown option, malformed
value) is caught by the same `SanitizedArgumentParser` pattern already used
by `promoted_engine_cli.py`/`promoted_graph_publisher_cli.py`: it raises
instead of printing argparse's usage text, so it flows through the same
boundary as an ordinary runtime failure -- exit code 2 with only
`{"status": "FAILED", "error_type": "..."}`, never a raw argument value,
path, ID, or parser message. `--help` is unaffected.

## Paper-only meaning and explicit non-goals

Every published `PromotedResearchRunManifest` has `paper_only: true`,
`notification_eligible: false`, and `execution_eligible: false`
permanently -- regardless of how many research intents were selected.
Combined success proves only that one deterministic paper research pass
was durably assembled and bound to its exact upstream graph; it never
proves any signal, score, or intent is authorized for a live decision.

This bridge does not send a notification or alert, does not place an order
or size a live position, does not mutate a broker/GCP/live store, does not
schedule anything, and does not choose a model, an auto-tuned parameter, or
a "latest" graph/engine run -- every input is an exact, caller-supplied ID.
It does not modify the promoted graph publisher or the promoted engine
themselves; both run entirely unchanged underneath this bridge.
