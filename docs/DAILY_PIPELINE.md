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

This closes the local reader-to-job composition. What remains separate,
unauthorized future work: constructing a real `GoogleCloudStorageObjectReader`
against actual GCP credentials, IAM/bucket configuration, a CLI command,
a scheduler, and any deployment wiring. Nothing in this increment touches
any of those.

## Current limitations

- The manual `run` CLI command consumes manually downloaded, collection-only
  evidence via caller-supplied filesystem paths. `run_daily_pipeline_from_
  pinned_gcs_manifest` (the injected pinned-GCS composition boundary
  documented above) exists and is fully tested, but it is a Python entry
  point only -- there is no CLI command, no real
  `GoogleCloudStorageObjectReader` construction, no GCP credential/IAM/bucket
  configuration, no scheduler, and no deployment wiring for it yet. Both
  entry points remain manually invoked; neither is triggered automatically.
- The calendar is locally observed and not point-in-time verified.
- Independent upstream stores deliberately replay raw sources; the current real
  two-vintage run takes several minutes on the development machine.
- Missing report vintages remain blockers. For example, the present archive
  lacks `REG1_IND150726.csv`, so 16 July effective surveillance state is not
  substituted from another date.
- Scheduling, authorized downloading, cloud object immutability, monitoring,
  and notifications are not implemented by either entry point.
