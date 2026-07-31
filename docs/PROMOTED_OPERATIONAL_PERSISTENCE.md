# Promoted operational persistence and publication service

`src/india_swing/promoted_operational_persistence.py` and
`src/india_swing/promoted_operational_service.py` are the restart-safe local
publication layer around the accepted `PromotedOperationalRunResult`. They
derive one immutable advisory for every `COMPLETE`/`FAILED` result, derive
and create-once register a paper trade only for a singular `COMPLETE`
`PAPER_BUY`, publish every side effect idempotently, and seal one compact
terminal record last. Neither module modifies the accepted runner/decision/
allocation/quote-gate/preparation types, the paper-ledger, or any legacy
operations/recommendation module.

## Artifact schemas

`PromotedOperationalAdvisoryRecord` binds `spec_id`, `result_id`,
`target_session`, `status`, the `PAPER_BUY`/`NO_TRADE` `action`,
`evaluated_at` (when available), `decision_id`/`package_id` (present only
for `COMPLETE`), the canonical `failure_codes` (present only for `FAILED`),
the exact `advisory_text`/`advisory_sha256`, and fixed
`paper_only=True`/`notification_eligible=False`/`execution_eligible=False`.
For a `COMPLETE` result the text/hash/action/evaluated_at/decision_id/
package_id are an unedited, byte-exact copy of the retained
`PromotedOperationalDecisionPackage`'s own advisory -- never reformatted.
For a `FAILED` result the text is deterministically derived from nothing
but this record's own retained scalar fields (opening with the exact
`PAPER RESEARCH ONLY — MANUAL REVIEW REQUIRED — DO NOT AUTO-EXECUTE` line,
then `Status: FAILED`, the target session, spec/result IDs, every canonical
failure code, and an explicit no-order/no-notification/no-execution
statement), so `verify_content_identity` can genuinely re-derive and
compare it byte-for-byte even without the live result.

`PromotedOperationalTerminalRecord` is a compact publication summary, not
independent provenance and not a serialization of the entire runner graph.
It retains `spec_id`, `result_id`, `target_session`, `status`, `action`,
`started_at`/`evaluated_at`/`completed_at`, the two source IDs,
`preparation_id`, and the optional `quote_batch_id`/`portfolio_context_id`/
`portfolio_snapshot_id`/`quote_gate_batch_id`/`allocation_batch_id`/
`decision_id`/`package_id` -- mirroring the runner result's own exact
stage-prefix depth rather than re-deriving a separate one, since a valid
`PromotedOperationalRunResult` already enforces it. It always carries the
mandatory `advisory_id`/`advisory_sha256` and the canonical `failure_codes`,
plus an optional `paper_registration_id`.

## Paper-registration mapping

`promoted_paper_registration_from_result(result, advisory)` is a pure
adapter returning `None` unless `result` is an exact `COMPLETE` run whose
decision action is `PAPER_BUY`. Every mapped value comes only from retained
result/advisory fields -- nothing is invented, widened, or coerced:

| `PaperTradeRegistration` field | Source |
|---|---|
| `alert_id` | `advisory.advisory_id` |
| `source_run_id` | `result.result_id` |
| `source_pipeline_integrity_hash` | the candidate's `preparation_id` |
| `source_decision_integrity_hash` | `decision.decision_id` |
| `signal_id` | `candidate.candidate_id` |
| `symbol` / `quantity` | `recommendation.symbol` / `recommendation.quantity` |
| `decision_time` | `result.evaluated_at` |
| `earliest_entry_at` / `entry_expires_at` | the quote-gate spec's own decision window |
| `entry_low` | `outcome.reference_entry_price` (observed best ask) |
| `entry_high` | the research limit price |
| `stop` / `target` / `max_holding_sessions` | the retained research levels |
| `estimated_round_trip_cost` | `outcome.estimated_round_trip_cost` |

If any ordering or lineage condition is not satisfied, `PaperTradeRegistration`'s
own validation rejects construction; nothing is coerced to make it fit.

## Directory layout

Under one caller-supplied root:

```text
advisories/<advisory_id>.json          # LocalPromotedOperationalAdvisoryOutbox
advisories/.promoted-operational-advisory.lock
terminals/<spec_id>.json               # LocalPromotedOperationalTerminalStore
terminals/.promoted-operational-terminal.lock
```

The caller-supplied `LocalPaperTradeLedger` (already accepted, unmodified)
manages its own separate root with its own `registrations/`/`events/`
layout.

## Create-once semantics

Both new stores follow the same hardened pattern already used by
`LocalSwingDecisionOutbox`/`LocalSwingOperationalRunStore`/
`LocalPaperTradeLedger`: reject symlinks/reparse points/non-regular files,
use a persistent advisory lock, write a same-directory temporary file,
flush and fsync, create the final file with a hard-link/create-once
operation, remove the temporary, and read back and verify before success.
Existing identical bytes are idempotent (`put` of the same artifact twice
returns the same stored value and never rewrites the file); any conflicting
content at the same content-derived path fails closed. Neither store ever
overwrites or selects a "latest" artifact.

The strict JSON codecs require UTF-8, an exact envelope/field set, exact
codec/schema versions, duplicate-key rejection at every level,
`parse_float`/`parse_constant` rejection (no float or NaN/Infinity
literals anywhere in the document), canonical date/datetime parsing,
lowercase-SHA-256 ID formats, exact enum/boolean values, and byte ceilings
(128 KiB for an advisory file, 512 KiB for a terminal file). Every decode
reconstructs the typed model through its own public constructor (so every
`__post_init__` semantic check re-runs), requires the stored ID to match
the freshly computed one, and then re-encodes the reconstructed value and
requires the result to match the original bytes exactly -- a non-canonical
but otherwise "valid" encoding (different key order, extra whitespace,
inconsistent formatting) is rejected, not silently accepted.

`LocalPromotedOperationalTerminalStore.get_optional(spec_id)` returns
`None` only when nothing at all exists at the exact canonical path (no
file, no symlink, broken or otherwise). Any other condition -- a
directory or non-regular file in the way, a symlink, a permission or read
error, invalid JSON, tampered content, or an identity mismatch -- raises a
sanitized `PromotedOperationalStoreError` and is never treated as
absence. Directory listing/latest-selection is intentionally not provided
by either store.

## Publication ordering and terminal-last sealing

`publish_promoted_operational_result` writes side effects strictly in
order: the advisory first, then -- only for a singular `COMPLETE`
`PAPER_BUY` -- the exact `PaperTradeRegistration` through the caller-
supplied `LocalPaperTradeLedger`, and the terminal record last. `FAILED`
and `COMPLETE` `NO_TRADE` publication never requires or writes a paper
registration. If advisory or required registration publication fails
(including a `PAPER_BUY` result published without a ledger), no terminal
record is ever written -- the absence of a terminal is itself the signal
that publication did not complete.

## Sealed-terminal replay and the crash/retry matrix

`run_and_publish_promoted_operational_service` checks
`terminal_store.get_optional(spec.spec_id)` *before* reading either
`source_id` property or invoking the clock:

| State found | `terminal_binding` supplied | Behavior |
|---|---|---|
| No terminal | `None` | Runs `execute_promoted_operational_run` exactly once, then `publish_promoted_operational_result`. Caller must durably anchor the returned `terminal_id` outside this local bundle before acknowledging the run. |
| No terminal | non-`None` | Fails closed immediately -- an anchored terminal that isn't locally present is an inconsistency, checked before any source property, acquisition, clock, advisory, or ledger access. |
| Terminal exists, exact binding matches, spec-bound and advisory/registration verify | matching | Returns the existing artifacts. **No clock read, no `source_id` property read, no acquisition call of any kind.** |
| Terminal exists, binding missing, malformed, foreign-`spec_id`, or `expected_terminal_id` mismatch | anything else | Fails closed before `advisory_outbox.get` or any `paper_ledger` access. |
| Terminal exists, binding matches, but does not structurally match the supplied spec | matching | Fails closed. Never re-runs the market pipeline to "repair" it. |
| Terminal exists, binding matches, referenced advisory missing/corrupt/cross-linked | matching | Fails closed with a sanitized error. |
| Terminal exists, binding matches, `paper_registration_id` present but no ledger supplied, or the referenced registration is missing/corrupt/cross-linked | matching | Fails closed the same way. |

A terminal is the point of no reacquisition: once sealed, this service
never fetches a new quote or portfolio state to fill in a gap. A crash
between advisory publication and terminal sealing is safe to retry --
`publish_promoted_operational_result` re-derives the identical advisory/
registration/terminal from the same live result and the stores' own
idempotent `put` reuses what's already there, sealing the terminal exactly
once.

## Individual content hashes are not sufficient: centralized bundle and spec-binding verification

A content-addressed hash only proves a record is internally
self-consistent with *itself* -- it does not prove the record is
consistent with its *siblings* or with the live spec a caller supplies on
replay. Because `LocalPromotedOperationalTerminalStore` keys its file by
`spec_id` (a stable key that does not change when the terminal's content
changes), an attacker or corruption bypassing the store's own `put()`
conflict check can overwrite the terminal file directly with a *different*
but still self-consistent terminal (a freshly recomputed `terminal_id` for
the new content) while leaving an untouched, still-valid sibling advisory
in place. Two independent defenses close this gap:

- `_verify_terminal_matches_spec(terminal, spec)` binds the terminal's
  compact `status`/`action`/`target_session`/`preparation_id`/source-ID/
  time/failure-code/stage-depth shape to the exact live `spec`, replaying
  `PromotedOperationalRunResult`'s own stage-prefix truth table (which
  failure code implies which exact depth, evaluation/completion window,
  and source-pinning state) against the terminal's own fields. A forged
  terminal that could never have been produced by *this* spec -- wrong
  session, wrong preparation, unpinned sources, an implausible depth for
  its claimed candidate count, or a failure code paired with the wrong
  stage depth -- is rejected here even though its own hash recomputes
  correctly.
- `verify_promoted_operational_published_bundle(terminal, advisory,
  registration)` is a centralized cross-artifact verifier requiring
  `spec_id`/`result_id`/`target_session`/`status`/`action`/`evaluated_at`/
  `decision_id`/`package_id`/`advisory_id`/`advisory_sha256`/
  `failure_codes`/authority flags to agree between the terminal and its
  referenced advisory, and (when a registration is referenced)
  `registration_id`/`alert_id`/`source_run_id`/
  `source_pipeline_integrity_hash`/`source_decision_integrity_hash`/
  `decision_time` to agree between the registration and the terminal, with
  registration presence/absence exactly matching the terminal's `action`.
  This closes the concrete exploit Codex reproduced: a terminal
  self-consistently rewritten from `COMPLETE`/`PAPER_BUY` to
  `FAILED`/`NO_TRADE` while still pointing at the original, untouched
  `COMPLETE`/`PAPER_BUY` advisory now disagrees with that advisory on
  `status`/`action`/`evaluated_at`/`decision_id`/`package_id` and is
  rejected. `PromotedOperationalPublishedState.__post_init__` calls this
  verifier on every construction (fresh publish and sealed replay alike),
  and the sealed-replay path in `run_and_publish_promoted_operational_service`
  additionally checks the referenced registration's
  `earliest_entry_at`/`entry_expires_at` against the *live* spec's own
  decision window before returning.
- `build_promoted_operational_terminal_record` calls both verifiers on the
  terminal it just built, before returning, as defense-in-depth on the
  fresh-publish path too.

Neither verifier claims cryptographic authentication or independent
provenance -- they verify every relationship the sealed artifacts and the
supplied spec make checkable, nothing more, and they still never
reacquire a quote or portfolio state.

Compact prefix shape is strengthened further: `PromotedOperationalTerminalRecord`
now rejects `portfolio_context_id` present without `portfolio_snapshot_id`
(or vice versa) unconditionally, and `_verify_terminal_matches_spec` rejects
any terminal that reaches `portfolio_context_id`-or-deeper without
`quote_batch_id` for a spec with candidates (the runner always acquires the
quote batch before portfolio context, on every status), and rejects
`quote_batch_id` present at all for a zero-candidate spec. Previously the
deepest-truthy-field depth ladder alone could be fooled by a terminal that
dropped an *earlier* required ID while keeping every *later* one -- the
apparent depth still read as complete.

## Individual content hashes cannot authenticate a coordinated rewrite: the trusted terminal binding

The verifiers above compare the local terminal against its own referenced
local advisory/registration and against the live spec. They cannot detect
an attacker (or corruption) that rewrites *both* the terminal and its
advisory together, self-consistently, so the two local artifacts still
agree with each other on every field the bundle verifier checks -- Codex
reproduced exactly this: a `COMPLETE`/`PAPER_BUY` publication rewritten to
`FAILED`/`NO_TRADE` by canonically re-encoding both files in tandem, which
`run_and_publish_promoted_operational_service` then accepted and returned
without ever touching a source. No amount of additional public content
hashing inside the local bundle can close this gap, because the attacker
controls every hash-verified field on both sides.

`TrustedPromotedOperationalTerminalBinding(spec_id, expected_terminal_id)`
is the fix: a caller-supplied trust anchor from an *independent*,
durably-retained control plane -- not content-addressed provenance, and
never derived by this service from `terminal_store`. `run_and_publish_-
promoted_operational_service` accepts an optional `terminal_binding`
parameter:

- On a **fresh** call (no local terminal yet), `terminal_binding` must be
  `None`. The caller is responsible for durably anchoring the returned
  terminal's `terminal_id` *outside* this local bundle -- in the future
  production adapter, the GCP control plane -- before acknowledging the
  run as complete.
- On a **replay** call (a local terminal already exists), an exact
  `terminal_binding` is mandatory: `binding.spec_id` must equal both the
  live `spec.spec_id` and the existing terminal's own `spec_id`, and
  `binding.expected_terminal_id` must equal the existing terminal's own
  `terminal_id` -- checked immediately after loading the terminal and
  strictly before `advisory_outbox.get` or any `paper_ledger` access. A
  rewritten local terminal (coordinated or not) always has a different
  `terminal_id` than the one that was originally anchored, so this check
  alone closes the coordinated-rewrite gap the bundle verifier cannot.

This service never constructs a `TrustedPromotedOperationalTerminalBinding`
from `terminal_store` itself -- doing so would make the anchor and the
thing it is meant to authenticate the same untrusted local artifact and
defeat the entire trust boundary. A crash after the local terminal is
created but before the caller has durably anchored its `terminal_id`
outside this bundle is deliberately a fail-closed, manual-recovery state:
there is no automatic way to infer the correct anchor from local state
alone, and this module adds none. Durably retaining the trusted binding is
explicitly out of scope for this local persistence layer; it is the
responsibility of the future, explicitly authorized production adapter and
its own GCP generation/provenance rules.

## CLOCK_NON_MONOTONIC compatibility

The accepted `PromotedOperationalRunResult` explicitly permits
`evaluated_at < started_at` when its `failure_codes` include
`CLOCK_NON_MONOTONIC` -- the failure code exists precisely to retain, for
audit, an evaluation timestamp that violated ordering.
`PromotedOperationalTerminalRecord._verify()` mirrors this exactly: it
only relaxes the `evaluated_at >= started_at` requirement when
`CLOCK_NON_MONOTONIC` is present in the terminal's own `failure_codes`,
and unconditionally still requires `completed_at >= evaluated_at`. Every
valid accepted runner result -- including a `CLOCK_NON_MONOTONIC` `FAILED`
result whose evaluation-depth or completion-depth prefix truthfully
predates `started_at` -- therefore remains publishable and replayable.

## Non-goals

Local advisory creation is not Telegram delivery and grants no
notification or execution authority -- every advisory and terminal record
is `paper_only=True`, `notification_eligible=False`,
`execution_eligible=False` permanently. Neither module imports Telegram,
any HTTP/network client, a Kite adapter, GCP, environment variables,
credentials, `subprocess`, or a broker order API, and neither reads the
real clock. Production adapters and explicitly authorized Telegram/GCP job
wiring remain the next milestone.
