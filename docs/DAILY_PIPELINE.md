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
There is no CLI command, no scheduler, no IAM/Cloud Run wiring, no
notification, and no composition with `acquire_verified_landing_inputs` or
`run_daily_pipeline_from_landing_manifest` yet -- wiring this manifest
acquisition boundary into the daily landing job so a manifest no longer has
to be supplied as pre-verified bytes remains separate future work.

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

## Current limitations

- The runner consumes manually downloaded, collection-only evidence.
- The calendar is locally observed and not point-in-time verified.
- Independent upstream stores deliberately replay raw sources; the current real
  two-vintage run takes several minutes on the development machine.
- Missing report vintages remain blockers. For example, the present archive
  lacks `REG1_IND150726.csv`, so 16 July effective surveillance state is not
  substituted from another date.
- Scheduling, authorized downloading, cloud object immutability, monitoring,
  and notifications are not implemented by this local command.
