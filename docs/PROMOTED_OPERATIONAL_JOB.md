# Promoted operational job entrypoint

`src/india_swing/promoted_operational_job.py` is the first
production-shaped, paper-only process entrypoint for the promoted paper
engine. It is the missing composition root sitting directly above the
already-accepted assembly and runtime layers (see
[`docs/PROMOTED_OPERATIONAL_ASSEMBLY.md`](PROMOTED_OPERATIONAL_ASSEMBLY.md)
and [`docs/PROMOTED_OPERATIONAL_RUNTIME.md`](PROMOTED_OPERATIONAL_RUNTIME.md)):
it accepts one exact local assembly-spec file plus explicit durable
roots, deterministically assembles the already-approved pinned
preparation and portfolio artifacts, constructs a Kite quote source and
one shared injected GCS client, invokes the accepted promoted operational
runtime exactly once, and emits one small sanitized audit result on
stdout. It never reproduces any financial calculation, risk rule,
sequencing, or persistence codec already owned by the accepted layers.

Console script: `india-swing-promoted-operational-job` (declared in
`pyproject.toml`, mapped to `india_swing.promoted_operational_job:main`).

**This module never sends Telegram output, never places an order, never
performs interactive/browser login or token refresh, and never deploys
anything. Deployment (Cloud Run Job wiring, IAM, scheduling) remains a
later, separately reviewed increment -- this document describes the
process shape only.**

## Command shape

```text
india-swing-promoted-operational-job \
  --assembly-spec-file /abs/path/to/assembly.json \
  --reference-root /abs/path/to/reference \
  --identity-evidence-root /abs/path/to/identity-evidence \
  --calendar-root /abs/path/to/calendar \
  --daily-reports-root /abs/path/to/daily-reports \
  --historical-corpus-root /abs/path/to/historical-corpus \
  --promoted-root /abs/path/to/promoted \
  --graph-publication-root /abs/path/to/graph-publication \
  --engine-run-root /abs/path/to/engine-run \
  --research-run-root /abs/path/to/research-run \
  --operational-preparation-root /abs/path/to/operational-preparation \
  --portfolio-artifact-root /abs/path/to/portfolio-artifact \
  --state-root /abs/path/to/state
```

Every option is required exactly once. Unknown, duplicate, or missing
options fail closed. Every value must be a concrete, absolute,
non-traversing path (no relative path, no `..` component); none of these
paths are required to pre-exist -- the underlying accepted store
constructors create their own subdirectories lazily on first write,
exactly as they already do outside this entrypoint. **The GCS bucket
comes only from the verified assembly spec's own `binding_bucket` field
-- there is no `--bucket` CLI option and no bucket-related environment
variable.**

## Required environment

| Variable | Purpose |
|---|---|
| `INDIA_SWING_KITE_API_KEY` | Zerodha Kite Connect API key (placeholder only -- never a real value in this repository or its tests). |
| `INDIA_SWING_KITE_ACCESS_TOKEN` | Daily Kite access token from the existing, already-authorized login flow. This entrypoint only ever consumes an already-issued token via `KiteCredentials.from_env` -- it never performs interactive/browser login or a token-refresh flow itself. |

Both are read only once, only from the process environment (or an
injected mapping in tests), and only after every local assembly step
(spec loading, preparation/portfolio resolution, and the full assembly
cross-check) has already succeeded.

## State subdirectories

Given `--state-root <root>`, the job constructs exactly:

- `<root>/advisory` -- `LocalPromotedOperationalAdvisoryOutbox`
- `<root>/terminal` -- `LocalPromotedOperationalTerminalStore`
- `<root>/paper` -- `LocalPaperTradeLedger`

These three fixed subdirectory names are never configurable, never
discovered, and never a "latest" selection.

## Exact composition order

1. Parse and validate every CLI argument.
2. Load and strictly decode the exact assembly-spec file
   (`load_promoted_operational_assembly_spec_file`, unchanged).
3. Build the preparation resolver from the ten explicit roots
   (`build_promoted_operational_preparation_store`, unchanged) and the
   portfolio-artifact resolver (`LocalSwingPortfolioArtifactStore`,
   unchanged).
4. Call `assemble_promoted_operational_runtime_inputs` (unchanged) --
   this independently re-verifies the spec and both resolved parents
   before returning anything.
5. Resolve every production default (clock, Kite-adapter factory,
   GCS-client factory, runtime callable) and require **all four are
   callable** before invoking any of them or constructing any GCS/
   state-root adapter -- capability validation only; this never calls the
   clock or the runtime itself. A non-callable injected dependency (a
   local configuration mistake) therefore fails before credentials are
   even loaded, and leaves the GCS factory uncalled and every state-root
   component unconstructed.
6. Load Kite credentials from the exact environment mapping
   (`KiteCredentials.from_env`).
7. Construct the Kite adapter (injected factory, or
   `KiteMarketDataAdapter.from_official_sdk` in production) and wrap it
   in the unchanged `KiteSwingQuoteSource`.
8. Independently require `quote_source.source_id ==
   assembly.runtime_job_spec.expected_quote_source_id`.

**Only after step 8 succeeds** does the job touch anything GCS-shaped or
construct a state-root store:

9. Call the GCS-client factory exactly once (injected, or
   `google.cloud.storage.Client()` in production). **A `None` result is
   rejected immediately** with the static sanitized error, before either
   GCS adapter is constructed -- the accepted `GoogleCloudStorageStateObjectWriter`
   otherwise treats `client=None` as permission to construct its own
   ambient default client, which would silently violate the one-shared-client
   contract if a faulty factory ever returned `None`. Once a non-`None`
   client is confirmed, inject that exact same object into exactly one
   `GoogleCloudStorageStateObjectWriter(client=client)` and exactly one
   `GoogleCloudStorageTerminalBindingReader(client)`. The reader instance
   is used for **both** `binding_reader` and `binding_preflight` -- there
   is no second client, no listing, no "latest" selection, no repair, no
   deletion, and no fallback anywhere in this module.
10. Construct the three state-root stores (see above) and
    `PinnedPromotedOperationalPortfolioSource(assembly.portfolio_context)`.
11. Call the runtime (injected callable, or the unchanged
    `run_promoted_operational_runtime_job` in production) **exactly
    once**, with the assembled job/run specs, both pinned sources, the
    same clock, the three stores, and the shared-client control-plane
    adapters.
12. Independently verify the returned result (see
    [Defensive result check](#defensive-result-check)) and emit the
    success envelope.

This is one bounded invocation: no loop, scheduler, retry, auto-repair,
deletion, or latest selection exists anywhere in this module. A replay
(an already-anchored terminal for the same spec) may still construct the
read-only Kite quote source for the identity check in step 8 -- the
accepted runtime, not this entrypoint, is what decides whether quote
acquisition is actually skipped for that replay.

## Injectable seams

`main` exposes four small factory/dependency parameters --
`clock`, `kite_adapter_factory`, `gcs_client_factory`, and
`runtime_callable` -- so tests can exercise the complete composition
without monkeypatching SDK modules or touching the network/GCP.
Production defaults are resolved *inside* `main` at call time (never at
function-definition time), so the injectable seam and the production
path share the exact same call sites. A test's `kite_adapter_factory` can
legitimately construct a real `KiteMarketDataAdapter` around a harmless
fake client object (`KiteMarketDataAdapter(fake_client, sdk_version=...)`)
-- this exercises the exact same `KiteSwingQuoteSource` identity
machinery as production without ever installing or importing the
`kiteconnect` package or touching a socket.

## Defensive result check

Before emitting any success output, the job requires the runtime's
return value to be exactly `PromotedOperationalRuntimeState`, then
independently verifies (under one sanitized boundary, so a malicious or
malformed injected result can never leak a foreign exception or produce
a false success) the **complete** published bundle and binding -- not
only selected cross-links. Trusting individual content identities alone
is insufficient: independently rewritten terminal and advisory records
can each be internally self-consistent while disagreeing with one
another. A stale single-field tamper such as an `advisory_id` change must
also fail the affected record's identity replay. The check therefore:

- Requires `result.job_spec.job_spec_id` matches
  `assembly.runtime_job_spec.job_spec_id`, via
  `result.job_spec.verify_content_identity()`.
- Calls the accepted, unchanged
  `verify_promoted_operational_published_bundle(terminal, advisory,
  paper_registration)`, which independently replays the terminal's and
  advisory's own content identity **and** requires every mutually
  checkable field between them to agree (`advisory_id`, `advisory_sha256`,
  `spec_id`, `result_id`, `target_session`, `status`, `action`,
  `evaluated_at`, `decision_id`, `package_id`, `failure_codes`, and all
  three authority flags) -- exactly the check that catches a terminal
  whose `advisory_id` was tampered independently of its referenced
  advisory.
- Calls `binding_record.verify_content_identity()`, then independently
  **rebuilds the expected binding record** from the terminal and the
  assembled `run_spec` via the accepted, unchanged
  `build_promoted_operational_terminal_binding_record`, and requires the
  returned `binding_record` equals that freshly rebuilt record exactly
  (covering `binding_id`, `spec_id`, `target_session`, `preparation_id`,
  `expected_terminal_id`, and `terminal_completed_at`) -- never trusting
  the returned record's selected fields in isolation.
- Independently recomputes the expected binding object name via the
  accepted `promoted_operational_terminal_binding_object_name(assembly.run_spec)`
  and requires `result.anchored.binding_object_name` matches it, and
  requires `result.anchored.binding_bucket` matches
  `assembly.runtime_job_spec.binding_bucket`.
- Requires `result.anchored.published.reused_existing_terminal is
  result.anchored.reused_existing_terminal` (wrapper coherence), and that
  `binding_generation`/`reused_existing_terminal` are the expected types
  and internally coherent (`binding_generation > 0`).
- Requires the anchored terminal's `spec_id`/`preparation_id`/
  `target_session` match the assembled `run_spec`/`assembly_spec`, and
  its `paper_only`/`notification_eligible`/`execution_eligible` flags
  remain exactly `True`/`False`/`False`.

## Success envelope

On success, the job writes exactly one canonical, compact, sorted-key
JSON object to stdout (and nothing to stderr), containing only:

```text
status, assembly_spec_id, runtime_job_spec_id, operational_run_spec_id,
preparation_id, target_session, terminal_id, terminal_status, action,
failure_codes, advisory_id, binding_id, binding_generation,
reused_existing_terminal, paper_only, notification_eligible,
execution_eligible
```

`status` is always the literal `"PROMOTED_OPERATIONAL_JOB_COMPLETE"`.
Enums are serialized by their `.value`; `target_session` is an ISO date
string. Every value is derived only from the independently re-verified
assembly/runtime result -- no path, bucket, object name, credential,
quote value, portfolio holding, or exception text/repr is ever included.

## Failure envelope

Any argument, filesystem, decoding, assembly, credential,
SDK-construction, identity, GCS-construction, store-construction,
runtime, nested-result, or serialization failure writes no stdout,
writes exactly one compact sorted-key JSON object to stderr, and returns
exit code 2:

```json
{"error_type":"PromotedOperationalJobError","status":"FAILED"}
```

The underlying exception is never chained (`raise ... from`) and never
inspected for its message/repr before this static object is written --
this collapses every failure category into the identical, sanitized
shape.

## Replay behavior

A second invocation with the same `--state-root` (and the same assembly
spec) is a *replay*: the accepted runtime finds the already-published
local terminal and its already-sealed remote binding, and the success
envelope's `reused_existing_terminal` is `true` with the same
`terminal_id`/`binding_id` as the first, fresh invocation. This
entrypoint performs no special-casing for replay itself -- it always
calls the runtime exactly once and lets the accepted runtime own that
entire truth table, exactly as documented in
`docs/PROMOTED_OPERATIONAL_RUNTIME.md`.

## Cloud Run Job suitability

The command shape (all configuration via explicit flags, all secrets via
environment variables, one bounded invocation, no daemon/scheduler loop,
a single small JSON result on stdout, and a single small JSON error on
stderr with a non-zero exit code) is suitable for a Cloud Run Job
execution -- one job execution per invocation, no server process, no
open port. **No Cloud Run, IAM, scheduling, or deployment configuration
is created by this task**; wiring an actual Cloud Run Job (or any other
deployment target) around this entrypoint is a later, separately
reviewed increment.

## Non-goals

- No Telegram, email, or webhook delivery of any kind.
- No broker order placement, modification, or cancellation.
- No interactive/browser Kite login and no token-refresh flow -- only an
  already-issued daily access token via `KiteCredentials.from_env`.
- No scheduler, daemon, retry, or auto-repair loop.
- No filesystem discovery, listing, or "latest" selection anywhere.
- No second GCS client, no bucket listing, no object deletion or repair.
- No deployment, Cloud Run/IAM/scheduling configuration, or live-capital
  authority of any kind.
