# Promoted operational runtime boundary

`src/india_swing/promoted_operational_runtime.py` is the first
production-shaped, fully dependency-injected runtime boundary for the
promoted paper engine. It sits directly above the already-accepted chain
(research preparation, quote gate, allocation, decision package,
restart-neutral runner, terminal-last local publication, independent
create-once terminal binding, GCS-shaped preflight/read/write ports, and
the fail-closed anchored session) and does not redesign any of it. This
module never constructs a real storage/broker/Telegram client, never reads
an environment variable or credential, never makes a network call, and
never reads the current time. Loading the promoted run spec from upstream
stores, constructing real Kite/GCS clients from credentials, Telegram
delivery, CLI/Cloud Run wiring, scheduling, and a real shadow run remain
the next, separately authorized increment.

## The trust boundary this module closes

The promoted runner accepts two narrow ports plus a clock, and the
anchored session accepts persistence/control-plane dependencies, but
until this module there was no strict job-level artifact that bound all
of those identities together *before* a run. Tests constructed
`PromotedOperationalRunSpec` and portfolio contexts directly in memory,
with nothing forcing a caller-supplied run spec and caller-injected
sources to agree with a separately retained job description.

`PromotedOperationalRuntimeJobSpec` is that binding. It is deliberately
**not a second engine spec** -- it never encodes or rebuilds the deeply
nested `PromotedOperationalRunSpec`. It retains only the exact run-spec
ID plus redundant, safety-critical cross-links: the preparation ID, the
target session, the decision window, both pinned source IDs, the pinned
portfolio context ID, and the terminal-binding bucket. The caller injects
the exact `PromotedOperationalRunSpec` object; `run_promoted_operational_runtime_job`
independently re-verifies it and requires every retained cross-link to
agree before touching the clock, either source's acquisition method,
either local store, or the control plane.

## Job-spec schema

`PromotedOperationalRuntimeJobSpec` is frozen/slots and content-addressed:

| Field | Type | Notes |
|---|---|---|
| `operational_run_spec_id` | `str` (lowercase SHA-256) | Must equal the live `PromotedOperationalRunSpec.spec_id` at call time. |
| `preparation_id` | `str` (lowercase SHA-256) | Must equal `run_spec.quote_gate_spec.preparation.manifest.preparation_id`. |
| `target_session` | `date` | Must equal `run_spec.quote_gate_spec.preparation.manifest.target_session`; also cross-checked against the decision window in IST at construction. |
| `decision_not_before` | `datetime` (literal zero UTC offset) | Must equal `run_spec.quote_gate_spec.decision_not_before`. A nonzero-offset aware datetime (e.g. IST) is **rejected outright**, never normalized/coerced -- see [Canonical UTC, not normalized UTC](#canonical-utc-not-normalized-utc). |
| `decision_deadline` | `datetime` (literal zero UTC offset) | Must equal `run_spec.quote_gate_spec.decision_deadline`; `decision_not_before < decision_deadline`, and both map to `target_session` in `INDIA_STANDARD_TIME`. |
| `expected_quote_source_id` | `str` (lowercase SHA-256) | Must equal both `run_spec.expected_quote_source_id` and the live `quote_source.source_id`. |
| `expected_portfolio_source_id` | `str` (lowercase SHA-256) | Must equal both `run_spec.expected_portfolio_source_id` and the pinned `portfolio_source.source_id`. |
| `expected_portfolio_context_id` | `str` (lowercase SHA-256) | Must equal the pinned `portfolio_source.context_id`. |
| `binding_bucket` | `str` | Strict GCS bucket-name shape; the sole source of truth passed to the anchored session -- there is no separate `binding_bucket` parameter to the orchestrator. |
| `paper_only` / `notification_eligible` / `execution_eligible` | `bool` | Always `True` / `False` / `False`. |
| `schema_version` | `str` | Fixed `promoted-operational-runtime-job-spec/v1`. |
| `job_spec_id` | `str` (`field(init=False)`) | `content_id(...)` over every other field. |

`verify_content_identity()` **reconstructs** a fresh instance from the
object's own retained field values (`PromotedOperationalRuntimeJobSpec(**self._identity())`),
rerunning every validation in `__post_init__`, and then requires the
re-derived ID to match -- it never merely compares a caller-supplied hash
against a stored one.

`build_promoted_operational_runtime_job_spec(*, run_spec, portfolio_context,
binding_bucket)` is the only pure builder: it requires exact types, calls
`run_spec.verify_content_identity()` and `portfolio_context.verify_content_identity()`,
requires `portfolio_context.source_portfolio_artifact_id ==
run_spec.expected_portfolio_source_id` before building, and derives every
retained field only from those two already-verified inputs. It never
reads a clock, source, file, environment, GCS object, or store.

### Canonical UTC, not normalized UTC

`decision_not_before`/`decision_deadline` must already be represented in
UTC (`utcoffset() == timedelta(0)`) at construction time. Unlike several
sibling modules' `_require_aware_utc` helpers, this module's
`_require_literal_utc` never calls `astimezone(timezone.utc)` to coerce an
equivalent instant under a different offset (for example IST) into the
canonical form -- it rejects it outright with the same static
`PromotedOperationalRuntimeError`. This matters because the identical
instant under two different offset representations would otherwise be
silently accepted at construction yet produce two different
non-byte-identical canonical JSON encodings, undermining the "one
canonical representation" guarantee the strict codec depends on.
`build_promoted_operational_runtime_job_spec` is unaffected in practice:
`run_spec.quote_gate_spec.decision_not_before`/`decision_deadline` are
already normalized to UTC by the accepted `PromotedOperationalQuoteGateSpec`,
so every valid builder output and its `job_spec_id` are unchanged.

## Strict canonical codec and file loader

`encode_promoted_operational_runtime_job_spec`/`decode_promoted_operational_runtime_job_spec`
follow the exact pattern already established by
`promoted_terminal_binding.py`'s codec: a fixed maximum of
`MAXIMUM_RUNTIME_JOB_SPEC_BYTES` (64 KiB), strict UTF-8, duplicate-key
rejection at every JSON object level (`object_pairs_hook`), exact key
sets at both the envelope and body level, `parse_float`/`parse_constant`
hooks that reject any float/NaN/Infinity token anywhere in the payload,
canonical ISO date/UTC-datetime string representations, re-verification
of the stored `job_spec_id` against the freshly constructed object's own
recomputed ID, and a final byte-for-byte re-encode check so even a
semantically-equivalent-but-differently-formatted payload (for example, a
decision timestamp written in IST rather than canonical UTC) is rejected
as noncanonical rather than silently accepted.

`load_promoted_operational_runtime_job_spec_file(path)` requires `path` to
be **exactly** the platform's concrete `pathlib.Path` subclass --
`type(path) is _CONCRETE_PATH_TYPE` where `_CONCRETE_PATH_TYPE =
type(Path())` (`WindowsPath` on Windows, `PosixPath` elsewhere) -- not an
`isinstance` check. An arbitrary `Path` subclass instance (which could
override `is_absolute()` or any other path method) is rejected before any
of its behavior is ever consulted. The path must also be absolute and
non-traversing (rejects any path containing a `..` component), and the
loader reuses `read_stable_regular_file` unchanged, which independently
rejects symlinks/reparse points and concurrent mutation. Every failure --
a relative path, a traversing path, a subclassed path, a missing file, a
directory, a symlink, an oversized file, or tampered content -- collapses
to the same static sanitized `PromotedOperationalRuntimeError`. There is
no filesystem discovery, no list/latest/nearest/glob behavior, no
fallback, and no default path.

## Reuse of `KiteSwingQuoteSource`; no second Kite wrapper

`src/india_swing/operations/runner.py` already provides
`KiteSwingQuoteSource`, whose `source_id` is
`content_id(adapter.identity_material, length=64)` and whose
`fetch_full_quotes` has the exact signature `PromotedOperationalQuoteSource`
requires. This module accepts the `PromotedOperationalQuoteSource`
**protocol** structurally -- it never imports, constructs, or duplicates
`KiteSwingQuoteSource` or `KiteMarketDataAdapter`. A later, separately
authorized job entrypoint injects the real `KiteSwingQuoteSource`.

## Pinned portfolio semantics

`PromotedOperationalPortfolioContext` already binds one verified
`SwingPortfolioSnapshot`, its exact `source_portfolio_artifact_id`, and
its exact sorted `open_listing_keys`.
`PinnedPromotedOperationalPortfolioSource` wraps exactly one such
already-verified context and independently re-calls
`context.verify_content_identity()` both at construction and on every
subsequent read (`source_id`, `context_id`, `read_portfolio_context()`),
so later in-place tampering -- or a context that was corrupted before
being handed to this source -- is rejected with the same static
sanitized error rather than silently trusted. It never infers open
listings, queries a broker, mutates the context, or synthesizes a
replacement.

## Cross-check and call order

`run_promoted_operational_runtime_job` is a thin orchestrator. Before
calling `run_publish_and_anchor_promoted_operational_session` -- and
before touching the clock, either source's acquisition method, either
local store, or the control plane -- it:

1. Requires `job_spec` to be its exact type and calls
   `job_spec.verify_content_identity()`, catching any nested failure and
   converting it to the static `PromotedOperationalRuntimeError` (see
   [Uniform public error boundary](#uniform-public-error-boundary) below).
2. Requires `run_spec` to be its exact type and calls
   `run_spec.verify_content_identity()` under the same defensive wrapping
   -- a tampered `run_spec` that would otherwise raise its own
   `PromotedOperationalRunnerError` is caught here and never escapes this
   module's boundary. This happens before `quote_source.source_id`,
   either source's other identity, the preflight, the clock, acquisition,
   or either local store is ever touched.
3. Requires `portfolio_source` to be the exact
   `PinnedPromotedOperationalPortfolioSource` type.
4. Requires `callable(clock)`, `type(advisory_outbox) is
   LocalPromotedOperationalAdvisoryOutbox`, `type(terminal_store) is
   LocalPromotedOperationalTerminalStore`, and `paper_ledger` to be either
   `None` or exactly `LocalPaperTradeLedger` -- checked by type only,
   never by calling any of them, and still before either source's
   identity is read.
5. Reads `quote_source.source_id` through a sanitized safe boundary
   (flag-then-raise-outside-except; any exception or non-`str` result
   fails closed) -- this is the one identity read this module performs on
   the caller-injected, potentially-untrusted quote source, and it
   happens on **every** call, fresh or replay, because the job spec's
   contract requires it to match.
6. Reads `portfolio_source.source_id`/`.context_id` (safe: this module's
   own pinned type, which only re-verifies already-retained local data).
7. Requires every job-spec cross-link to agree with the live `run_spec`
   (spec ID, preparation ID, target session, decision window, both
   expected source IDs) and with the two sources' own live identities
   (`quote_source.source_id`, `portfolio_source.source_id`/`.context_id`).
   Any disagreement raises `PromotedOperationalRuntimeError` immediately,
   with nothing else attempted.
8. Calls `run_publish_and_anchor_promoted_operational_session` **exactly
   once**, passing the caller's exact `run_spec`, both sources, the
   clock, the stores, the ledger, `job_spec.binding_bucket`, and the
   binding writer/reader/preflight straight through. This function never
   calls either source's acquisition method itself and never
   re-implements the accepted runner's ordering or call counts --
   `run_publish_and_anchor_promoted_operational_session` remains the sole
   caller of `run_and_publish_promoted_operational_service`. Binding
   writer/reader/preflight remain structural injected ports and receive
   no speculative method calls from this module.
9. Wraps the returned `AnchoredPromotedOperationalSessionState` together
   with `job_spec` into `PromotedOperationalRuntimeState`, whose own
   `__post_init__` independently re-verifies both and requires every
   cross-link mutually available between them to agree: job/run-spec ID,
   preparation ID, target session, portfolio source/context IDs *when
   present* on the retained terminal record, the binding bucket, and
   paper-only authority.

No retry, fallback, reconstruction, deletion, or repair exists anywhere
in this module.

### Uniform public error boundary

Every public constructor/function/method in this module raises exactly
one static, sanitized `PromotedOperationalRuntimeError` (never a nested
exception's own type -- `PromotedOperationalRunnerError`,
`PromotedOperationalAllocationError`, `AttributeError`, `TypeError`,
`OSError`, or any other injected exception) and never chains via `raise
... from` or by raising while still inside the `except` block that
caught the underlying failure, so `__cause__` and `__context__` are both
`None` on every rejection. This is enforced with the established
flag-then-raise-outside-except pattern throughout: a boolean flag (and,
where relevant, a captured `PromotedOperationalRuntimeError` instance for
re-raising as-is) is set inside a narrow `try`/`except Exception`, and the
actual `raise` statement always appears after that block has exited, so
Python's implicit exception chaining never activates.
`PromotedOperationalRuntimeState.__post_init__` applies this even to its
own nested-attribute cross-check against `self.anchored.published.terminal`
-- a malformed or tampered `anchored` value can never leak a foreign
exception type or context from that comparison.

### Why the decision window isn't re-checked inside `PromotedOperationalRuntimeState`

`PromotedOperationalRuntimeState` holds only `job_spec` and `anchored` --
never the caller's `PromotedOperationalRunSpec`. Neither
`AnchoredPromotedOperationalSessionState` nor the
`PromotedOperationalTerminalRecord` it retains carries a decision-window
field, so there is nothing downstream to re-derive it from at that point.
The decision window is instead pinned and cross-checked against the
caller's exact nested run spec *earlier*, at step 7 above, before the
anchored session is ever called -- consistent with the wealth-protection
invariant that decision window (along with every other identity) is
pinned before any clock/source/store/control-plane method runs, not
re-verified after the fact from data that was never retained.

## Paper-only authority

Every authority flag on `PromotedOperationalRuntimeJobSpec` is fixed:
`paper_only=True`, `notification_eligible=False`,
`execution_eligible=False`. `__post_init__` rejects any other value. This
module introduces no notification or order authority; it only binds
already-paper-only artifacts together.

## Restart behavior is entirely delegated

This module never branches on local-terminal presence, never reads
`terminal_store` itself, and never decides fresh-vs-replay -- that
complete truth table is owned exclusively by
`run_and_publish_promoted_operational_service`, reached transitively
through the single `run_publish_and_anchor_promoted_operational_session`
call. On a genuine restart with an already-sealed binding, no clock read
and no quote/portfolio *acquisition* call happens at the layers beneath
this one. This module's own preflight (steps 5/6 above) still reads both
sources' identity properties on every call, fresh or replay, because the
job-spec contract requires that agreement to be proven every time -- that
is a deliberate difference from the accepted anchored-session/service
layers beneath it, which skip even the identity-property reads on replay.
Reading a pinned identity property is not acquisition: it never touches a
broker, a clock, or unretained data.

## Non-goals

- No real storage, Kite, or Telegram client is constructed anywhere in
  this module.
- No credential, environment variable, or session token is read.
- No network call, subprocess, or current-time read exists here.
- No filesystem discovery, listing, "latest", or glob behavior exists in
  the file loader.
- No CLI, Cloud Run/Scheduler wiring, or deployment configuration exists
  here.
- No candidate reranking, price mutation, policy widening, quantity
  resizing, or open-listing inference happens anywhere in this module --
  all of that remains owned by the already-accepted chain this module
  only binds together and cross-checks.
