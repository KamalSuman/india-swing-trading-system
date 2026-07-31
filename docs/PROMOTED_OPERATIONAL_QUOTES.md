# Promoted operational quote gate

This is the second bounded increment in the research-to-paper-operation
seam: a pure, injected-data quote evaluation that binds one already-verified
`VerifiedPromotedOperationalPreparation` to one already-acquired, exact
`FullQuoteBatch` and produces one PASS/VETO outcome per retained candidate.
It evaluates quotes only -- it never acquires a quote, ranks a candidate,
sizes a position, produces a BUY decision, notifies, registers a paper
trade, or executes.

## Shared quote-quality evaluator

`src/india_swing/signals/quote_quality.py` extracts the proposal-independent
market-quality checks that used to live only inside the legacy Swing quote
gate: decision-window closure, quote/last-trade timeliness, two-sided
depth, spread, and circuit lock. `evaluate_quote_quality(...)` is pure --
it never modifies a quote or a price level -- and returns canonical sorted
unique string reason codes plus the observed spread. A listing-key mismatch
or a malformed parameter is an integrity error (raised), never folded into
the returned reason codes.

`src/india_swing/signals/quote_gate.py`'s private `_evaluate_quote_gate`
now calls this shared evaluator internally and then layers its own
proposal-specific `LAST_PRICE_OUTSIDE_ENTRY_RANGE`/
`BEST_ASK_OUTSIDE_ENTRY_RANGE` checks and quote-adjusted level calculation
on top. Every public class, enum value, schema version, policy ID, outcome
ID, batch ID, reason string, threshold inclusivity, and exception behavior
of the legacy gate is unchanged.

## Promoted operational quote gate

`src/india_swing/promoted_operational_quote_gate.py` adds three
content-addressed types:

- `PromotedOperationalQuoteGateSpec` binds one exact preparation, an
  explicit `decision_not_before`/`decision_deadline` decision window (both
  normalized to UTC, required to be strictly ordered, and required to map
  in Asia/Kolkata to the preparation's own `target_session`), and one exact
  `SwingQuoteGatePolicy`. `paper_only` is always true and both
  `notification_eligible`/`execution_eligible` are always false.
- `PromotedOperationalQuoteOutcome` retains one exact candidate, the quote
  evaluated against it, the spec, and the evaluation instant. It calls the
  same shared `evaluate_quote_quality` evaluator, then adds
  promoted-intent-native vetoes: `BEST_ASK_ABOVE_LIMIT`,
  `BEST_ASK_AT_OR_BELOW_STOP`, `BEST_ASK_AT_OR_ABOVE_TARGET`, and
  `QUOTE_TICK_MISMATCH` (checked against `last_price`/`best_bid`/`best_ask`
  and the retained intent's own `tick_size`). A missing best ask is already
  covered by the shared two-sided-depth reason and never crashes the
  intent-native checks. A PASS outcome carries no reasons, a non-null
  observed spread, and `reference_entry_price` equal exactly to
  `quote.best_ask`; a VETO carries sorted unique reasons and no reference
  price. Neither outcome ever changes quantity, limit, stop, target, cost
  buffer, reward/risk, holding period, or any other retained research-intent
  field.
- `VerifiedPromotedOperationalQuoteGateBatch` retains the exact spec, the
  optional quote batch, the evaluation instant, and the exact ordered
  outcome tuple. `verify_content_identity` independently replays every
  outcome from the spec and quote batch and rejects reordered, missing,
  duplicated, foreign, or forged outcomes.

`evaluate_promoted_operational_quote_gate(spec=..., quote_batch=..., evaluated_at=...)`
is the only entry point. For a nonempty preparation it requires one exact
`FullQuoteBatch` whose `requested_keys` exactly equal the **sorted** unique
listing keys drawn from `preparation.manifest.listing_keys`, and whose
quotes exactly cover those keys; it rejects a quote batch observed after
the evaluation time and a collection duration above
`policy.maximum_batch_collection_seconds` as integrity failures. It never
fetches or discovers a quote. A zero-candidate preparation is valid only
with `quote_batch=None` and yields an exact zero-outcome batch with
`pass_count=0`/`veto_count=0`; supplying a quote batch for a zero-candidate
preparation fails closed.

**Sorted quote transport vs. preserved outcome order.** These are two
separate contracts. `FullQuoteBatch.requested_keys` must already be sorted
(an existing, unrelated constraint of that type), but a preparation's
candidate order is not guaranteed to be alphabetical -- it preserves the
research intent batch's own canonical order. This module therefore
requests quotes over the *sorted* key set (transport canonicalization
only) while every produced `PromotedOperationalQuoteOutcome` is still
emitted in the exact, unsorted `preparation.candidates` order; no
candidate is ever reranked, dropped, or reordered to match the sorted
transport.

**Spec-membership.** `PromotedOperationalQuoteOutcome.verify_content_identity`
independently requires its retained `candidate` to be an exact member of
`spec.preparation.candidates` -- its `candidate_id` must occur exactly once
there, and the matching retained candidate must compare equal after its own
content-identity verification. A self-consistent candidate that simply
belongs to a *different* preparation can never form a valid standalone
outcome bound to another spec.

**UTC canonicalization.** `PromotedOperationalQuoteOutcome.evaluated_at` and
`VerifiedPromotedOperationalQuoteGateBatch.evaluated_at` are normalized to a
literal zero UTC offset at construction, and `verify_content_identity`
independently re-checks that literal zero-offset representation (not just
the represented instant) on every call -- so `object.__setattr__` replacing
either with an equivalent, non-UTC-offset datetime, even paired with a
freshly recomputed content ID, still fails.

**Context-independent tick alignment.** Tick-multiple checking
(`QUOTE_TICK_MISMATCH`) is computed from each Decimal's own
`as_tuple()` coefficient/exponent scaled to plain Python integers, never
from a Decimal `%`/`/` operator whose result can depend on the caller's
ambient `decimal` context (precision, rounding). The same aligned/misaligned
verdict holds regardless of what precision the caller's global context is
set to.

Every `verify_content_identity` method reconstructs or replays semantic
invariants rather than merely hashing current fields, so post-construction
`object.__setattr__` tampering followed by recomputing an ID still fails:
the spec's own window/session/authority invariants are re-run on
reconstruction, and the outcome's disposition/reasons/spread/reference
price/spec-membership/UTC-representation are re-derived or re-checked from
the retained candidate, quote, and spec rather than trusted from the
tampered fields themselves.

## Non-goals

This module does not add persistence, a CLI, network access, a Kite
adapter, scheduling, a current-time read, portfolio/risk/ranking/sizing, a
BUY/decision object, a notification, or paper registration. It does not
create or coerce a `SwingProposalBatch`/`SwingTechnicalProposal` from a
promoted intent. Quote acquisition and decision execution remain a later
milestone.
