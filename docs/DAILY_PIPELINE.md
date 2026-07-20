# Daily collection pipeline

The daily pipeline turns one explicit NSE CM session into a sealed collection
report. It orchestrates the existing strict importers and materializers; it does
not promote manual files, assign stable identities, generate a trade, or execute
an order.

## Bias boundary

The runner never scans a folder for the newest file and never selects an
implicit latest artifact. Every successor names one exact `previous_run_id`.
The supplied calendar must prove that predecessor is the immediately preceding
trading session. The predecessor carries forward the exact ordered security-
master and daily-bundle artifact chains, after which the current artifacts are
appended. A duplicate, skipped session, future cutoff, mismatched master date,
or missing current-session Bhavcopy fails closed.

A first run may omit `--previous-run-id`. It is explicitly marked
`NO_PREVIOUS_DAILY_RUN` and is only a bootstrap collection diagnostic. If a
calendar was first observed after the market session, the run cutoff must remain
later than that calendar knowledge time; the runner will not backdate it.

## One-command derivation

The `run` command:

1. imports the exact current security master and Multiple File Download ZIP;
2. derives positive traded-date evidence for the current bundle;
3. materializes the current raw, unadjusted EOD session;
4. reconciles the current master against the complete predecessor bundle chain;
5. rebuilds the ordered cross-vintage identity registry;
6. publishes the complete identity-adjudication queue;
7. writes one create-once, content-addressed completeness report.

The report is stored under `var/daily_pipeline/runs` by default. Override this
with `INDIA_SWING_DAILY_PIPELINE_ROOT`. The report binds every upstream artifact
ID, output ID, count, blocker, cutoff, calendar, and predecessor run. Reads
verify the report's own content identity and reject links, unexpected files,
duplicate JSON keys, mutation, or a path/content mismatch. Upstream artifact
stores retain their own raw-byte replay contracts.

Browser duplicate names such as `Reports-Daily-Multiple (1).zip` are accepted
only through the pinned `Reports-Daily-Multiple (positive integer).zip` pattern.
The source is read with stable-file checks and copied to a private temporary
official basename before the strict bundle importer runs. The original file is
never modified.

## Commands

Bootstrap one already-collected session:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.daily_pipeline.cli run `
  --session 2026-07-15 `
  --cutoff 2026-07-16T15:45:00+05:30 `
  --calendar-id <calendar-materialization-id> `
  --security-master-file C:\path\to\NSE_CM_security_15072026.csv.gz `
  --daily-bundle-file C:\path\to\Reports-Daily-Multiple.zip
```

Run the immediately following session:

```powershell
python -m india_swing.daily_pipeline.cli run `
  --session 2026-07-16 `
  --cutoff 2026-07-16T20:07:55+05:30 `
  --calendar-id <calendar-materialization-id> `
  --security-master-file C:\path\to\NSE_CM_security_16072026.csv.gz `
  --daily-bundle-file "C:\path\to\Reports-Daily-Multiple (1).zip" `
  --previous-run-id <exact-15-July-run-id>
```

Inspect persisted reports without rerunning the expensive upstream derivation:

```powershell
python -m india_swing.daily_pipeline.cli show --run-id <run-id>
python -m india_swing.daily_pipeline.cli list
```

Run one pinned-GCS session from an operator-authored spec file:

```powershell
python -m india_swing.daily_pipeline.cli run-pinned-gcs --spec-file C:\path\to\pinned-run-spec.json
```

`run-pinned-gcs` takes exactly one argument, `--spec-file`, passed through as a
raw string with no path normalization. Every run parameter -- session, cutoff,
calendar ID, manifest generation, trusted binding, and previous run -- comes
only from that operator-authored spec file; the command line cannot supply or
override any of them. The command composes its local stores (reference,
daily-bundle, historical-price, identity-registry, adjudication-queue,
run, and calendar-materialization) from the same `*_ROOT` environment
variables and constructor arguments the `run` subcommand uses, constructs one
real `GoogleCloudStorageObjectReader()` with no arguments, and delegates
exactly once to `run_daily_pipeline_from_pinned_gcs_run_spec_file`. It performs
no derived-evidence step -- run `derive --run-id <run-id>` afterward for tick,
liquidity, and universe evidence, exactly as after `run`.

CLI failures expose only a sanitized exception type. No partial daily-run report
is published when an upstream stage fails.

## Internal verified-landing-inputs entry point

`run_daily_pipeline_from_landing_inputs` (in `daily_pipeline/runner.py`) is a
second, internal entry point alongside `run_daily_pipeline`. It consumes an
exact, already-verified `VerifiedLandingInputs` value instead of caller-
supplied filesystem paths.

Before any artifact-store mutation it independently re-verifies the supplied
`VerifiedLandingInputs` (rejecting a mutated or wrong-type value), requires its
`market_session` and `run_cutoff` to equal the requested run's session and
cutoff, and builds the run's `LandingInputLineage` via
`build_landing_input_lineage`. Only then does it materialize the two already-
verified byte payloads into a private temporary directory, using the canonical
NSE basenames taken from that freshly built lineage and exclusive file
creation, and hand them to the same import/derive pipeline stages
`run_daily_pipeline` uses. It never lists a bucket, selects a "latest" object,
retries, or falls back to a second source.

Runs produced this way persist the exact `LandingInputLineage` on
`DailyPipelineRun` and omit `VERIFIED_LANDING_LINEAGE_UNAVAILABLE` from
`completeness_issues`. `run_daily_pipeline`'s manual-file behavior is
unchanged: `landing_input_lineage` remains `None` and the blocker remains
present.

This is an internal integration boundary only. There is no CLI command and no
production GCS wiring yet -- constructing a real `LandingObjectReader`,
resolving a manifest, and calling `acquire_verified_landing_inputs` from a
command remain separate future work.

## Landing-input lineage versioning: legacy v1 and manifest-source v2

`LandingInputLineage` (in `daily_pipeline/landing_lineage.py`) has two
schema-version shapes, both still readable and both still produced depending
on the exact inputs supplied:

- **Legacy v1** (`LEGACY_LANDING_INPUT_LINEAGE_SCHEMA_VERSION`, the same
  `"nse-cm-landing-input-lineage/v1"` string this schema has always used).
  `manifest_source` is required to be `None` on a v1 record, is excluded
  entirely from both the content-identity hash material and the serialized
  store JSON (the key is absent, not `null`), and every existing v1
  `lineage_id`/`run_id` computation is therefore byte-for-byte unchanged by
  this addition. `build_landing_input_lineage` still produces v1 whenever
  `VerifiedLandingInputs.manifest_acquisition` is `None` -- which is every
  case today, since neither the manual-file runner nor the
  caller-supplied-manifest job composes a real `AcquiredLandingManifest` yet.
- **V2** (`LANDING_INPUT_LINEAGE_SCHEMA_VERSION`, the new
  `"nse-cm-landing-input-lineage/v2"` string). Carries a required, non-`None`
  `manifest_source: LandingManifestSourceLineage` -- the exact `bucket`,
  `object_name`, `generation`, and `target_session` of the GCS manifest
  object an `AcquiredLandingManifest` was read from. It has no hash field of
  its own: `LandingInputLineage.manifest_sha256` remains the one retained
  manifest hash for both versions, so `manifest_source` never duplicates or
  becomes a second hash authority. `manifest_source` participates in the
  content-identity hash and the serialized JSON only for v2, and its bucket
  and session must agree with both retained data-object lineages and the
  lineage's own `target_session`. `build_landing_input_lineage` produces v2
  only when `VerifiedLandingInputs.manifest_acquisition` is present, after
  independently reconstructing and reverifying that acquisition record
  against the same trusted manifest bytes the rest of the lineage is built
  from.

`VerifiedLandingInputs` gained a final, defaulted
`manifest_acquisition: AcquiredLandingManifest | None = None` field, and
`acquire_verified_landing_inputs` gained a matching optional keyword. When
supplied, it is validated -- via a defensively reconstructed snapshot,
independently re-verified against `manifest` -- before either data-object
read, exactly like every other trust boundary in this package. Existing
callers that omit it are completely unaffected.

`LocalDailyPipelineRunStore` encodes/decodes both exact shapes: a legacy v1
record's stored field set has no `manifest_source` key at all, while a v2
record's field set always includes an exact nested `manifest_source` object;
neither shape can be silently reinterpreted as the other, and `DailyPipelineRun`
schema versions (`DAILY_PIPELINE_RUN_SCHEMA_VERSION`,
`DAILY_PIPELINE_RUN_STORE_SCHEMA_VERSION`) are untouched by this change.

This task adds only the lineage transport/storage model. It deliberately
does not compose `acquire_verified_landing_manifest` with the daily landing
job, add a CLI/scheduler, or construct a real GCS client -- wiring an actual
GCS-sourced manifest acquisition into `run_daily_pipeline_from_landing_manifest`
so a real run produces v2 lineage end-to-end remains separate future work.

## Internal landing-manifest object acquisition boundary

`acquire_verified_landing_manifest` (in `daily_pipeline/landing_manifest_acquisition.py`)
reads exactly one explicit, generation-pinned GCS landing-manifest object and
verifies it against an independently supplied `TrustedLandingManifestBinding`,
returning an exact `AcquiredLandingManifest`. It is the acquisition-layer
counterpart to `LandingManifestVerifier` (in `landing_manifest.py`), which
remains the single authority for manifest schema/session/temporal validation.

`AcquiredLandingManifest` is a frozen wrapper carrying exactly `request`
(the `LandingManifestObjectRequest` that produced the manifest) and
`manifest` (the resulting `VerifiedLandingManifest`); it duplicates none of
their fields. It exists because `VerifiedLandingManifest` alone discards the
manifest object's bucket, canonical object name, and pinned generation once
verification succeeds -- without the wrapper, nothing downstream could prove
which exact GCS generation supplied a given manifest, breaking exact source
lineage. Its `__post_init__` independently re-derives `manifest` from its own
retained `manifest_bytes` and `binding` and cross-checks `request` against
that reverified manifest's bucket and session, so a mismatched pairing or a
nested field mutated in place via `object.__setattr__` after construction is
rejected with one static, sanitized `LandingManifestAcquisitionError`.

The manifest object request is a distinct `LandingManifestObjectRequest` (in
`acquisition.py`) -- not a widened `LandingObjectRequest` -- with exactly
`bucket`, `object_name`, `generation`, and `target_session`; it carries no
expected hash and no `file_type`, because `TrustedLandingManifestBinding`
remains the single independent hash authority. The only accepted
`object_name` is the canonical `landing/{YYYY-MM-DD}/landing-manifest.json`,
derived from `target_session` the same way the existing security-master and
daily-bundle paths are derived; any other path (wrong session, path
traversal, backslash, absolute, browser-renamed, or Unicode-confusable
variant) is rejected before it can reach a request. The shared 64 KiB
verifier ceiling is exposed as the public `MAXIMUM_LANDING_MANIFEST_BYTES`
constant in `landing_manifest.py`, and both the acquisition boundary's own
payload-size check and `LandingManifestVerifier.verify`'s ceiling check use
that same constant -- there is no second, possibly-drifting size limit.

At function entry, `acquire_verified_landing_manifest` independently
reconstructs the exact request and binding it was given into
`request_snapshot` and `binding_snapshot` -- new, field-copied instances
decoupled from the caller's original objects -- and requires
`request_snapshot.bucket == binding_snapshot.allowed_bucket` and
`request_snapshot.target_session == binding_snapshot.target_session`. Every
subsequent step -- the reader call, the payload-generation check, the
expected-hash check, the verifier call, and the returned wrapper -- uses only
these snapshots and never re-reads the caller's original request or binding.
This is deliberate, not incidental: a reader (or concurrent caller) that
mutates the original request/binding objects in place via
`object.__setattr__` while `read_generation` is running cannot retroactively
change which bucket/object/generation was requested or which hash a
downloaded payload is checked against, so a reader cannot make a tampered
payload acceptable by mutating the original binding's expected hash during
the read -- a validation-to-use race that a discard-only reconstruction would
leave open.

Only after the snapshots are validated does the function call the injected
`GCSObjectReader.read_generation` exactly once, with the exact snapshot
bucket/object name/generation and `maximum_bytes=
MAXIMUM_LANDING_MANIFEST_BYTES`. The returned payload is independently
re-verified -- exact `GCSObjectPayload` type, exact non-bool matching
generation, non-empty bytes bounded by the shared limit, and SHA-256 equal to
`binding_snapshot.expected_manifest_sha256` -- before those exact bytes, and
only those bytes, are handed to `LandingManifestVerifier.verify`.

Ordinary failures (never `BaseException`) at each of the four stages --
request/binding validation, the reader call, payload validation, and
manifest verification -- are collapsed into one static, stage-specific,
chain-suppressed `LandingManifestAcquisitionError`; none of them expose
bucket/object names, generations, hashes, manifest bytes, or nested exception
text.

This boundary never constructs a GCS/storage client, never lists a bucket,
never selects a "latest" object, never retries, falls back, or substitutes a
second source, and never reads an environment variable or the current clock.
There is no CLI command, no scheduler, and no IAM/Cloud Run wiring or
notification. It is composed with `acquire_verified_landing_inputs` and the
daily pipeline stages by `run_daily_pipeline_from_pinned_gcs_manifest` (see
below); it is deliberately not composed with
`run_daily_pipeline_from_landing_manifest`, whose caller-supplied-bytes
contract cannot retain manifest source provenance.

## Internal landing-manifest job boundary

`run_daily_pipeline_from_landing_manifest` (in `daily_pipeline/landing_job.py`)
is a third, internal entry point that composes the existing trust-chain
stages in order: `LandingManifestVerifier.verify` -> exact manifest bytes and
an externally trusted `TrustedLandingManifestBinding` produce a
`VerifiedLandingManifest` -> `acquire_verified_landing_inputs`, using an
injected `LandingObjectReader`, produces a `VerifiedLandingInputs` ->
`run_daily_pipeline_from_landing_inputs` produces the `DailyPipelineRun`.

It does not reimplement manifest parsing, object acquisition, lineage
construction, file materialization, or daily pipeline stages; it only wires
the three existing functions together. The object reader and every
artifact store remain caller-injected. It never constructs a GCS client,
reads an environment variable, inspects the clock, lists objects, selects a
"latest" object, retries, falls back, schedules work, or sends a
notification -- session, cutoff, bucket, generation, hash, previous run, and
calendar are always explicit caller inputs, never inferred from filesystem
state, listings, environment variables, or current time.

A manifest or binding that fails verification is rejected before
`acquire_verified_landing_inputs` is ever called, so no object read happens
on an invalid manifest. A landing-input acquisition failure (including a
reader failure) is rejected before `run_daily_pipeline_from_landing_inputs` is
ever called, so no artifact-store mutation happens on a failed acquisition.
Ordinary failures at these two trust boundaries are each collapsed into one
static, stage-specific `DailyLandingJobError` with chaining suppressed;
neither manifest bytes, bucket/object names, generations, hashes, paths,
nested exception text, nor caller-supplied sentinel values can leak through
this boundary. Once verified landing inputs exist, this function defers
entirely to `run_daily_pipeline_from_landing_inputs` for validation, lineage,
persistence, and failure semantics -- it adds no rollback, cleanup, retry, or
cross-store transactionality of its own.

This is an internal integration boundary only. There is no CLI command, no
scheduler, and no authorized-download wiring yet.

## Internal pinned-GCS landing job composition (complete)

`run_daily_pipeline_from_pinned_gcs_manifest` (in
`daily_pipeline/gcs_landing_job.py`) is the fourth, internal entry point. It
completes the pinned-GCS reader-to-job composition: exactly one
caller-injected `GCSObjectReader` now drives the whole chain from an
explicit `LandingManifestObjectRequest` through to a persisted
`DailyPipelineRun` carrying exact v2 source lineage, with no client
construction anywhere in the path.

It composes, in this exact order:

1. `acquire_verified_landing_manifest(manifest_request, binding, reader)` --
   reads and verifies the pinned landing-manifest object, returning an
   `AcquiredLandingManifest`.
2. The same injected `reader` wrapped as `GCSLandingObjectReader(reader)` --
   the existing NSE-object reader in `acquisition.py` -- so only one
   `GCSObjectReader` implementation is ever constructed by the caller.
3. `acquire_verified_landing_inputs(manifest=acquired_manifest.manifest, ...,
   reader=GCSLandingObjectReader(reader), manifest_acquisition=
   acquired_manifest)` -- reads the two exact manifest-pinned NSE objects and
   retains the manifest acquisition, so `build_landing_input_lineage`
   produces v2 `LandingInputLineage` with an exact `manifest_source`.
4. `run_daily_pipeline_from_landing_inputs(...)` -- materializes,
   reconciles, and persists the run exactly as the existing verified-inputs
   boundary already does.

On a successful run this issues exactly three generation-pinned, bounded
reads through the injected reader, in order: the manifest (bounded by
`MAXIMUM_LANDING_MANIFEST_BYTES`), the security master (bounded by 32 MiB),
and the daily bundle (bounded by 128 MiB). It never calls
`run_daily_pipeline_from_landing_manifest` -- that boundary's caller-supplied
manifest bytes cannot carry GCS source provenance and would produce
legacy-v1 lineage instead. It never constructs a GCS/storage client, reads
an environment variable, inspects the clock, lists objects, selects a
"latest" object, retries, falls back to a second source, invokes a
subprocess, notifies, or schedules/deploys anything; every store, the
reader, and every temporal/calendar input remain caller-injected.

A manifest that fails acquisition is rejected before either NSE object is
read, so no artifact-store mutation happens on a failed manifest. A
data-object acquisition failure (including a `market_session`/`cutoff`
mismatch against the acquired manifest, or a reader failure on either
object) is rejected before `run_daily_pipeline_from_landing_inputs` is ever
called, preserving the existing security-master-before-daily-bundle read
order and fail-before-second-read behavior. Ordinary failures at the two new
trust boundaries each collapse into one static, stage-specific,
chain-suppressed `PinnedGCSLandingJobError`; once verified landing inputs
exist, `run_daily_pipeline_from_landing_inputs`'s own exceptions and
persistence semantics are unchanged.

This closes the local reader-to-job composition. A real
`GoogleCloudStorageObjectReader` and a CLI command now exist -- see
`run-pinned-gcs` below, which composes this function (via the spec-to-
calendar service seam and the spec-file boundary) rather than modifying it.
What remains separate, unauthorized future work: provisioning real GCP
credentials/IAM/bucket configuration, a scheduler, and any deployment
wiring.

## Pinned-GCS run specification: an operator-governed control document

`parse_pinned_gcs_run_spec` (in `daily_pipeline/pinned_gcs_run_spec.py`) is a
pure, strict, bounded parser and immutable value model for one JSON
document that provides every input `run_daily_pipeline_from_pinned_gcs_manifest`
needs: which exact GCS manifest generation to read, the independently
trusted binding to verify it against, and the run's session/cutoff/calendar
identity. It never reads a file, environment variable, or the current
clock, never constructs a GCS/storage client, never lists or selects a
"latest" object, and never infers a previous run.

**The parser cannot prove who authored this document.** Treat a parsed
`PinnedGCSRunSpec` as operator-governed input, not self-authenticating
evidence -- authenticity, IAM, and distribution of the underlying JSON
remain an operational control entirely outside this module's scope. In
particular, `trusted_binding.expected_manifest_sha256` must arrive through
that same independently governed operator/control-plane channel; this
parser never computes or derives it from manifest content, and no later
stage in this codebase is permitted to either -- the whole point of the
binding is that the expected hash is known *before* anything is fetched,
from a source independent of what gets fetched.

### Exact JSON schema

Top-level object, exactly these four keys:

| Key | Type | Notes |
|---|---|---|
| `schema_version` | integer | Must be exactly `1`. |
| `manifest_request` | object | See below. |
| `trusted_binding` | object | See below. |
| `run` | object | See below. |

`manifest_request`, exactly these four keys:

| Key | Type | Notes |
|---|---|---|
| `bucket` | string | Must equal `trusted_binding.allowed_bucket`. |
| `object_name` | string | Must be the canonical `landing/{session}/landing-manifest.json` path; no alias, wildcard, or traversal. |
| `generation` | integer | Positive signed-int64; not a bool, float, or string. |
| `target_session` | string | Canonical `YYYY-MM-DD`; must equal `trusted_binding.target_session` and `run.market_session`. |

`trusted_binding`, exactly these five keys:

| Key | Type | Notes |
|---|---|---|
| `expected_manifest_sha256` | string | Exact lowercase 64-hex. Operator-supplied; never computed from fetched bytes. |
| `allowed_bucket` | string | Must equal `manifest_request.bucket`. |
| `target_session` | string | Canonical `YYYY-MM-DD`. |
| `not_before` | string | RFC3339-like, must be `<=` `cutoff` below. |
| `cutoff` | string | RFC3339-like, must be `<=` `run.cutoff`. |

`run`, exactly these four keys:

| Key | Type | Notes |
|---|---|---|
| `market_session` | string | Canonical `YYYY-MM-DD`. |
| `cutoff` | string | RFC3339-like, must be `>=` `trusted_binding.cutoff`. |
| `calendar_materialization_id` | string | Exact lowercase 64-hex. |
| `previous_run_id` | string or `null` | Required key. `null` means a first/bootstrap run; never discovered or inferred. Otherwise exact lowercase 64-hex. |

All RFC3339-like datetime strings require `T`, explicit seconds, optional
1-6 fractional digits, and an explicit `Z` or `+/-HH:MM` offset (never a
naive/offset-less timestamp); every accepted offset is normalized to UTC on
the returned value.

### Clearly fake example

The values below are entirely made up for illustration -- they are not a
real bucket, hash, or calendar ID:

```json
{
  "schema_version": 1,
  "manifest_request": {
    "bucket": "example-fake-landing-bucket",
    "object_name": "landing/2026-07-20/landing-manifest.json",
    "generation": 1234567890123,
    "target_session": "2026-07-20"
  },
  "trusted_binding": {
    "expected_manifest_sha256": "0000000000000000000000000000000000000000000000000000000000aa",
    "allowed_bucket": "example-fake-landing-bucket",
    "target_session": "2026-07-20",
    "not_before": "2026-07-20T00:00:00Z",
    "cutoff": "2026-07-20T14:00:00Z"
  },
  "run": {
    "market_session": "2026-07-20",
    "cutoff": "2026-07-20T15:00:00Z",
    "calendar_materialization_id": "0000000000000000000000000000000000000000000000000000000000bb",
    "previous_run_id": null
  }
}
```

### What this is not

This is a parser and value-model boundary only. It does not read a spec
file from disk, construct a real `GoogleCloudStorageObjectReader` or any
GCP client, load a calendar materialization, construct any store, or invoke
`run_daily_pipeline_from_pinned_gcs_manifest` -- those seams live in
`pinned_gcs_run_file_boundary.py` and the `run-pinned-gcs` CLI subcommand
(both documented below), not in this parser module. IAM/credential
provisioning, a scheduler, and deployment remain separate future work
outside this repo's code.

## Internal pinned-GCS run-spec service seam

`run_daily_pipeline_from_pinned_gcs_run_spec` (in
`daily_pipeline/pinned_gcs_run_service.py`) is the fifth, internal entry
point. It is a pure, dependency-injected application-service boundary that
binds one already-validated `PinnedGCSRunSpec` to one exact,
replay-verified `StoredCalendarMaterialization` (from
`calendar_data/materialization_store.py`), independently revalidates and
cross-binds them, and only then delegates to
`run_daily_pipeline_from_pinned_gcs_manifest`.

This service does not prove authorship of `spec` or provenance of an
arbitrarily caller-constructed `calendar_materialization`. It never reads a
spec file, environment variable, or the current clock; never constructs
`LocalCalendarMaterializationStore`, `GoogleCloudStorageObjectReader`, or
any GCP client or local artifact/run store; never lists or selects a
"latest" calendar materialization; and never infers
`calendar_materialization_id` or `previous_run_id`. The outer CLI boundary
that reads a spec file through a bounded safe-file boundary and obtains
`calendar_materialization` only through
`LocalCalendarMaterializationStore.get(spec.calendar_materialization_id)`
remains separate future work.

Preflight, in order, before any GCS read is attempted:

1. `spec` must be exact `PinnedGCSRunSpec` (never a subclass or shaped
   proxy). A fresh `PinnedGCSRunSpec` is reconstructed from all seven
   retained fields, re-running that module's own exact-type validation, so
   a post-construction-mutated spec (or a mutated nested
   `manifest_request`/`trusted_binding`) cannot bypass it. Only this fresh
   snapshot is used afterward.
2. `calendar_materialization` must be exact `StoredCalendarMaterialization`.
   Its `manifest`, `materialization`, and `materialization.calendar_snapshot`
   are each required to be their exact expected types, then each
   independently calls its own `verify_content_identity()`.
3. `manifest.artifact_id` and `materialization.materialization_id` must both
   exactly equal the fresh spec's `calendar_materialization_id`;
   `manifest.calendar_snapshot_id` must exactly equal
   `calendar_snapshot.snapshot_id`; and `manifest`/`materialization` must
   explicitly agree on cutoff, coverage bounds, readiness/actionable state,
   schema/policy identity, and source/observed-evidence lineage. Two
   individually self-consistent (content-hash-valid) but mutually
   mismatched objects are rejected here, not just objects with a broken
   hash.
4. The fresh spec's `market_session` must be a declared trading session on
   `calendar_snapshot` (`calendar_snapshot.require_session(...)`), and
   `calendar_snapshot.cutoff` must not exceed the fresh spec's `cutoff`. No
   calendar lookup, latest selection, fallback, or inferred calendar ID
   exists in this module.

Only after every check above passes does this function call
`run_daily_pipeline_from_pinned_gcs_manifest` exactly once, with the fresh
spec's `manifest_request`, `trusted_binding` (as `binding`),
`market_session`, `cutoff`, `calendar_materialization_id`, and
`previous_run_id`; the verified `calendar_snapshot`; and the exact
caller-supplied `reader` and stores, unchanged.

Ordinary failures (never `BaseException`) collapse into one of three
static, sanitized `PinnedGCSRunServiceError` messages -- one for spec
reconstruction, one for calendar verification/mismatch, one for delegated-
job execution -- with chaining suppressed, so no bucket/path/hash/ID/date
value or nested exception text can leak through this boundary. This
function adds no retry, rollback, cleanup, alternate data source, or
partial-success semantics of its own.

This closes the spec-to-calendar binding seam. What remains separate,
unauthorized future work: reading a spec file from disk through a bounded
safe-file boundary, constructing a real `GoogleCloudStorageObjectReader`,
calling `LocalCalendarMaterializationStore.get(...)` to obtain
`calendar_materialization`, constructing real artifact/run stores, IAM/bucket
configuration, a CLI command, a scheduler, and any deployment wiring.
Nothing in this increment touches any of those.

## Internal pinned-GCS run-spec file boundary

`load_pinned_gcs_run_spec_file` and
`run_daily_pipeline_from_pinned_gcs_run_spec_file` (both in
`daily_pipeline/pinned_gcs_run_file_boundary.py`) are the sixth, internal
entry point. This boundary introduces exactly one new capability into the
package: bounded binary reading of one caller-named regular file. It adds
no environment, clock, client-construction, store-construction, listing/
latest, retry, CLI-argument, or scheduler capability.

`load_pinned_gcs_run_spec_file(spec_path)` requires `spec_path` to be a
non-empty `str` containing no NUL character, then requires -- via
`Path.lstat` plus the `stat` module -- that the target exists and is a
regular file; a symlink, directory, or other non-regular target is
rejected without ever being opened. At most
`MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES + 1` bytes are read and handed
unmodified to `parse_pinned_gcs_run_spec`, which remains the sole authority
on spec byte-size and schema validation -- this boundary defines no ceiling
of its own and never truncates.

`run_daily_pipeline_from_pinned_gcs_run_spec_file(spec_path, *,
calendar_store, reader, reference_store, daily_store, historical_store,
identity_store, adjudication_store, run_store)` loads the spec via
`load_pinned_gcs_run_spec_file`, requires `calendar_store` to be exact
`LocalCalendarMaterializationStore` (never a subclass), then calls
`calendar_store.get(spec.calendar_materialization_id)` exactly once -- the
only calendar lookup this function performs; there is no listing, latest
selection, fallback, retry, or inferred ID. Only after that acquisition
succeeds does it delegate exactly once to
`run_daily_pipeline_from_pinned_gcs_run_spec` with the loaded spec, the
acquired materialization, and the caller-supplied `reader`/stores
unchanged.

Ordinary path/filesystem/parse failures collapse into one static,
sanitized `PinnedGCSRunFileBoundaryError`; an ordinary calendar-store type
or `get` failure collapses into a second, separate static sanitized
`PinnedGCSRunFileBoundaryError`. Neither exposes path, bucket, hash, ID,
date, or nested exception text. The delegated call to
`run_daily_pipeline_from_pinned_gcs_run_spec` is never wrapped in
try/except: its `PinnedGCSRunServiceError` propagates unchanged, and no
`BaseException` is ever intercepted anywhere in this module.

This file boundary itself remains a Python entry point with no CLI wiring,
no real `GoogleCloudStorageObjectReader` construction, and no local store
construction from configuration -- see the `run-pinned-gcs` CLI subcommand
below, which is exactly that outer wiring, composed on top of this boundary
without modifying it.

## `run-pinned-gcs` CLI command (complete)

The `run-pinned-gcs` subcommand (in `daily_pipeline/cli.py`, documented in
"Commands" above) is the outer CLI wiring for the pinned-GCS spec-file
boundary. It accepts exactly one argument, `--spec-file`, passed through as
a raw string (no `type=Path`, no normalization -- `load_pinned_gcs_run_spec_
file` owns all path validation). Its `main()` branch builds the same
reference/daily-bundle/historical-price/identity-registry/adjudication-queue
stores the `run` branch already builds, from the same `Config.from_env()`
roots and the same constructor arguments (including the historical/identity
cross-root arguments); additionally builds `calendar_store =
LocalCalendarMaterializationStore(CalendarDataConfig.from_env().data_root,
daily_config.data_root)` -- passed as the store itself, never `.get()`-ed by
the CLI; constructs `reader = GoogleCloudStorageObjectReader()` with no
arguments (the production `storage.Client()` credential path); and calls
`run_daily_pipeline_from_pinned_gcs_run_spec_file` exactly once with the raw
`--spec-file` string and every constructed dependency. The returned
`DailyPipelineRun` is rendered through the same `_summary()`-based
`{"status": "COMPLETE", "kind": "DAILY_PIPELINE_RUN", ...}` shape the `show`
branch already uses -- no derived-evidence keys; run `derive --run-id
<run-id>` separately for tick/liquidity/universe evidence, exactly as after
`run`.

The branch never reads the spec file itself, never calls
`calendar_store.get`, never catches an exception locally (the existing
sanitized top-level `except Exception` -- unchanged -- is the only failure
path, printing only `{"status": "FAILED", "error_type": ...}` and returning
exit code 2), and never accepts or infers session/cutoff/calendar/previous-
run/generation values from the command line. No new environment variable,
default root, or config field was introduced; no existing subcommand,
argument, default, response shape, or exit code was changed.

This closes the CLI-wiring seam this side of IAM/scheduling/deployment. What
remains separate, unauthorized future work: provisioning real GCP
credentials/IAM/bucket configuration for `GoogleCloudStorageObjectReader`'s
production `storage.Client()` path, a scheduler, and any deployment wiring.
Nothing in this increment touches any of those.

## Current limitations

- `run-pinned-gcs` (documented above) is the only pinned-GCS entry point
  wired into the CLI; `run_daily_pipeline_from_pinned_gcs_manifest` (the
  injected pinned-GCS composition boundary) and
  `run_daily_pipeline_from_pinned_gcs_run_spec` (the spec-to-calendar
  service seam) remain internal Python entry points composed underneath it,
  not separately exposed as CLI commands. Real GCP credential/IAM/bucket
  provisioning, a scheduler, and deployment wiring do not exist yet for
  `run-pinned-gcs` or any other command.
- The calendar is locally observed and not point-in-time verified.
- Independent upstream stores deliberately replay raw sources; the current real
  two-vintage run takes several minutes on the development machine.
- Missing report vintages remain blockers. For example, the present archive
  lacks `REG1_IND150726.csv`, so 16 July effective surveillance state is not
  substituted from another date.
- Scheduling, authorized downloading, cloud object immutability, monitoring,
  and notifications are not implemented by either entry point.
