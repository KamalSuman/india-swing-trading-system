# Promoted operational assembly layer

`src/india_swing/promoted_operational_assembly.py` is the deterministic,
content-addressed assembly layer immediately below the future promoted
operational CLI. It sits directly above the already-accepted
preparation/quote-gate/allocation/runner/runtime chain and reconstructs,
from one strict spec plus two minimal exact-ID resolver ports, everything
`run_promoted_operational_runtime_job` (see
[`docs/PROMOTED_OPERATIONAL_RUNTIME.md`](PROMOTED_OPERATIONAL_RUNTIME.md))
needs to run. This module never constructs a real storage/broker/Telegram
client, never reads an environment variable, credential, or the current
time, never calls Kite/GCS/Telegram, never mutates a store, and never
executes a runtime session -- a CLI and the actual execution of the
assembled runtime remain the next, separately authorized increment.

## The trust boundary this module closes

The runtime orchestrator requires a caller-created `PromotedOperationalRunSpec`
and `PromotedOperationalPortfolioContext` already in memory; there is
intentionally no codec for the full nested run-spec graph (serializing it
would mean either duplicating the preparation/quote-gate/allocation
identity formulas or trusting an unverified blob). This module closes
that gap the other way: it retains only a *launch binding* -- exact
parent IDs, the exact decision window, the exact quote/allocation
policies, the quote-source ID, explicit open-listing keys, the chunk
ceiling, and the terminal-binding bucket -- and reconstructs every
downstream engine-layer object from that binding plus two freshly
resolved, independently re-verified parents. It never reimplements the
preparation, quote-gate, allocation, or portfolio-context validation,
calculations, risk rules, or identity formulas; it only binds and
cross-checks the already-accepted types.

## Assembly-spec schema

`PromotedOperationalAssemblySpec` is frozen/slots and content-addressed:

| Field | Type | Notes |
|---|---|---|
| `preparation_id` | `str` (lowercase SHA-256) | Must equal the resolved `VerifiedPromotedOperationalPreparation.manifest.preparation_id`. |
| `portfolio_artifact_id` | `str` (lowercase SHA-256) | Must equal the resolved `SwingPortfolioSnapshotArtifact.artifact_id`. |
| `expected_portfolio_snapshot_id` | `str` (lowercase SHA-256) | Must equal the resolved artifact's `portfolio_snapshot_id`. |
| `expected_quote_source_id` | `str` (lowercase SHA-256) | Threaded unchanged into the reconstructed `PromotedOperationalRunSpec`. |
| `open_listing_keys` | `tuple[str, ...]` | Canonical (sorted, unique, `NSE:...` shaped) explicit evidence of which listings are open -- see [Open-listing rule](#open-listing-rule) below. |
| `decision_not_before` / `decision_deadline` | `datetime` (literal zero UTC offset) | Never normalized from a non-UTC representation -- rejected outright, matching `promoted_operational_runtime.py`'s own convention. Must map to `target_session` in `INDIA_STANDARD_TIME`. |
| `target_session` | `date` | Must equal the resolved preparation's own `manifest.target_session`. |
| `quote_gate_policy` | `SwingQuoteGatePolicy` | Reused unchanged; re-verified via its own `verify_content_identity()`. |
| `allocation_policy` | `PromotedOperationalAllocationPolicy` | Reused unchanged, including its nested `SwingPortfolioSizingPolicy`; re-verified via its own `verify_content_identity()` (which itself replays the nested sizing policy). |
| `maximum_quote_chunk_size` | `int` | Bounded by the exact same `_MINIMUM_QUOTE_CHUNK_SIZE`/`_MAXIMUM_QUOTE_CHUNK_SIZE` constants imported directly from `promoted_operational_runner.py` -- never a separately duplicated ceiling. |
| `binding_bucket` | `str` | Strict GCS bucket-name shape; threaded straight through to `build_promoted_operational_runtime_job_spec`. |
| `paper_only` / `notification_eligible` / `execution_eligible` | `bool` | Always `True` / `False` / `False`. |
| `schema_version` | `str` | Fixed `promoted-operational-assembly-spec/v1`. |
| `assembly_spec_id` | `str` (`field(init=False)`) | `content_id(...)` over every other field, including the full nested policy structure. |

`verify_content_identity()` reconstructs a fresh instance from the
object's own retained field values (rerunning every validation in
`__post_init__`, including replaying both nested policy objects) and
requires the re-derived ID to match -- it never merely compares a
caller-supplied hash. This module defines its own private
`_require_literal_utc` helper rather than importing
`promoted_operational_runtime.py`'s private helper of the same shape,
specifically so a malformed datetime here always raises this module's
own static `PromotedOperationalAssemblyError` and never lets a foreign
`PromotedOperationalRuntimeError` escape this boundary.

## Open-listing rule

`SwingPortfolioSnapshot` intentionally retains only an open-position
*count*, never listing identities. `open_listing_keys` is therefore
explicit, caller-supplied evidence, never inferred from candidates,
symbols, filesystem contents, or the portfolio count itself.
`assemble_promoted_operational_runtime_inputs` requires
`len(spec.open_listing_keys) == portfolio.open_positions` **before**
constructing `PromotedOperationalPortfolioContext` (which independently
enforces the identical rule again on construction) -- a zero-position
portfolio accepts only an empty tuple, and a nonzero count requires
exactly that many canonical (sorted, unique, `NSE:...`-shaped) keys.

## Strict canonical codec and file loader

`encode_promoted_operational_assembly_spec`/`decode_promoted_operational_assembly_spec`
follow the exact pattern established by `promoted_operational_runtime.py`'s
job-spec codec: a fixed maximum of `MAXIMUM_ASSEMBLY_SPEC_BYTES` (16 KiB),
strict UTF-8, duplicate-key rejection at every JSON object level
(`object_pairs_hook`), exact key sets at every level, `parse_float`/
`parse_constant` hooks that reject any float/NaN/Infinity token anywhere
in the payload, canonical ISO date/UTC-datetime string representations,
re-verification of the stored `assembly_spec_id` against the freshly
constructed object's own recomputed ID, and a final byte-for-byte
re-encode check.

The two policy objects are **flattened**, never serialized as opaque
blobs: `SwingQuoteGatePolicy` and `PromotedOperationalAllocationPolicy`
(including its nested `SwingPortfolioSizingPolicy`) are each encoded into
their own existing primitive fields (Decimal fields as canonical strings,
via the same round-trip-equality check `str(Decimal(text)) == text` used
elsewhere in this codebase) plus their own computed IDs. Decoding
reconstructs each policy class through its real constructor and
independently compares the reconstructed object's own ID against the
stored one at every level -- a tampered nested field that still parses
but no longer matches its own policy's stored ID is rejected before it
ever reaches the outer assembly-spec construction.

`load_promoted_operational_assembly_spec_file(path)` requires `path` to
be **exactly** the platform's concrete `pathlib.Path` subclass
(`type(path) is _CONCRETE_PATH_TYPE` where `_CONCRETE_PATH_TYPE =
type(Path())`) -- not `isinstance` -- so an arbitrary `Path` subclass
with overridden behavior is rejected before any of it is consulted. The
path must also be absolute and non-traversing, and the loader reuses
`read_stable_regular_file` unchanged for symlink/reparse-point/
concurrent-mutation rejection. Every failure collapses to the same
static sanitized `PromotedOperationalAssemblyError`; there is no
filesystem discovery, list/latest/nearest/glob behavior, fallback, or
default path.

## Exact resolution and call order

Two minimal, ordinary (not `@runtime_checkable`) `Protocol`s expose only
exact-ID lookup -- no list/latest/nearest/find/glob API exists on either,
and the assembly function never accepts a filesystem path or root:

```python
class PromotedOperationalPreparationResolver(Protocol):
    def get(self, preparation_id: str) -> VerifiedPromotedOperationalPreparation: ...

class SwingPortfolioArtifactResolver(Protocol):
    def get(self, artifact_id: str) -> SwingPortfolioSnapshotArtifact: ...
```

The already-accepted `LocalPromotedOperationalPreparationStore` and
`LocalSwingPortfolioArtifactStore` both satisfy these structurally,
without modification.

`assemble_promoted_operational_runtime_inputs(*, spec, preparation_resolver,
portfolio_artifact_resolver)`:

1. Requires `spec` to be its exact type and calls
   `spec.verify_content_identity()` before either resolver is ever
   touched.
2. Calls `preparation_resolver.get(spec.preparation_id)` **exactly
   once**, then verifies it in a strict, safe order: first requires the
   exact `VerifiedPromotedOperationalPreparation` type without touching
   any nested field; only then calls `preparation.verify_content_identity()`
   inside its own flag-then-raise-outside-except boundary; only *after*
   that replay succeeds are `manifest.preparation_id` (must match what
   was requested) and `manifest.target_session` (must agree with
   `spec.target_session`) ever read -- and even then, inside their own
   safe boundary, so a manifest structurally corrupted after construction
   (e.g. its `manifest` attribute itself replaced) can never leak a
   foreign `AttributeError` or any other exception out of this function.
   All of this happens before the second resolver is ever called.
3. Calls `portfolio_artifact_resolver.get(spec.portfolio_artifact_id)`
   **exactly once**. Requires the exact `SwingPortfolioSnapshotArtifact`
   type, matching `artifact_id`/`portfolio_snapshot_id`, and independent
   re-verification.
4. Requires portfolio freshness: `decision_not_before -
   allocation_policy.maximum_portfolio_age_seconds <= portfolio.as_of <=
   decision_deadline` (mirroring the identical bound already established
   by `StoredSwingPortfolioSource.read_portfolio` in
   `operations/portfolio_store.py`).
5. Requires `len(spec.open_listing_keys) == portfolio.open_positions`
   (see [Open-listing rule](#open-listing-rule)) -- before any child
   object is constructed.
6. Constructs, in order, `PromotedOperationalPortfolioContext`,
   `PromotedOperationalQuoteGateSpec`, `PromotedOperationalRunSpec`, and
   (via the unchanged `build_promoted_operational_runtime_job_spec`) the
   `PromotedOperationalRuntimeJobSpec`.
7. Wraps everything into `PromotedOperationalRuntimeAssembly`, whose own
   `__post_init__` independently replays every retained object and
   cross-checks all available parent IDs, target session, decision
   window, both policies, source IDs, explicit open-listing keys, chunk
   size, bucket, and paper-only authority -- **including every field
   `PromotedOperationalRuntimeJobSpec` itself retains**, not only its
   `operational_run_spec_id` and `binding_bucket`. A content-valid
   runtime job artifact is never trusted merely because its own ID
   replays: `preparation_id`, `target_session`, `decision_not_before`/
   `decision_deadline`, `expected_quote_source_id`,
   `expected_portfolio_source_id`, `expected_portfolio_context_id`, and
   all three authority flags are each independently compared against the
   assembly spec, the reconstructed preparation/run-spec/portfolio-context,
   and the runtime job spec's own values, so a separately self-consistent
   but wrong runtime-job binding (for example, one built with a different
   `preparation_id` but otherwise identical `operational_run_spec_id` and
   `binding_bucket`) is rejected rather than silently accepted.

Any resolver exception, wrong return type, mismatched/tampered parent,
stale/future portfolio, listing-count mismatch, or construction failure
fails closed under the one static `PromotedOperationalAssemblyError` --
never retried, never falling back, never discovering another artifact,
never partially returning, and never calling either resolver more than
once. The second resolver is never reached if the first parent's own
identity check fails.

## Error boundary

Every public constructor/function/method raises exactly one static,
sanitized `PromotedOperationalAssemblyError` -- never a nested exception's
own type (`PromotedOperationalPreparationError`,
`SwingPortfolioArtifactError`, `PromotedOperationalAllocationError`,
`SwingQuoteGateError`, `SwingPortfolioSizingError`,
`PromotedOperationalRuntimeError`, `AttributeError`, `TypeError`,
`OSError`, or any other injected exception) -- and never chains via
`raise ... from` or by raising while still inside the `except` block that
caught the failure, so `__cause__` and `__context__` are both `None` on
every rejection. This is enforced throughout with the established
flag-then-raise-outside-except pattern.

## Paper-only authority

Every authority flag on `PromotedOperationalAssemblySpec` is fixed:
`paper_only=True`, `notification_eligible=False`,
`execution_eligible=False`. `__post_init__` rejects any other value, and
`PromotedOperationalRuntimeAssembly.__post_init__` cross-checks that
these flags still agree with the reconstructed `PromotedOperationalRunSpec`.
This module introduces no notification or order authority; it only binds
already-paper-only artifacts together.

## Non-goals

- No real storage, Kite, or Telegram client is constructed anywhere in
  this module.
- No credential, environment variable, or session token is read.
- No network call, subprocess, or current-time read exists here.
- No filesystem discovery, listing, "latest", or glob behavior exists in
  either resolver Protocol or the file loader.
- No CLI exists here, and this module never calls
  `run_promoted_operational_runtime_job` or executes a runtime session --
  it only produces the exact inputs that call would need.
- No candidate reranking, price mutation, policy widening, quantity
  resizing, or open-listing inference happens anywhere in this module --
  all financial calculation and validation remains exclusively owned by
  the already-accepted preparation/quote-gate/allocation/portfolio-context
  types this module only binds together and cross-checks.
