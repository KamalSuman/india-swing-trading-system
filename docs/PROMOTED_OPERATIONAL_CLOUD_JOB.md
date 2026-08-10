# Promoted operational Cloud Run wrapper

`src/india_swing/promoted_operational_cloud_control.py` and
`src/india_swing/promoted_operational_cloud_job.py` are a Cloud
Run-shaped, paper-only process boundary that composes the already-accepted
`promoted_operational_job` and `promoted_operational_gcs_state` layers
without reproducing any of their assembly, strategy, quote, allocation,
risk, sizing, terminal-binding, advisory, registration, or GCS-state
logic. Console entry point: `india-swing-promoted-operational-cloud-job`
(`india_swing.promoted_operational_cloud_job:main`).

**This increment performs no Cloud Run deployment, no IAM/Scheduler
configuration, no Telegram delivery, and no order placement.** Every
published/restored record remains `paper_only=True` and permanently
`notification_eligible=False` / `execution_eligible=False`.

## Control schema

`PromotedOperationalCloudRunControl` is one exact, content-addressed,
operator-authored launch control. It binds:

- `expected_assembly_spec_id`, `expected_operational_run_spec_id` -- exact
  SHA-256 identities the wrapper independently verifies against everything
  it reads before trusting it.
- `target_session`, `state_bucket`.
- `assembly_spec_file` plus the twelve exact path arguments
  `promoted_operational_job` already requires (ten preparation roots, the
  portfolio-artifact root, the state root) -- thirteen path fields total,
  each a concrete absolute non-traversing `pathlib.Path`, pairwise
  non-overlapping (checked purely by comparing path components, never by
  touching the filesystem).
- an optional `prior_state_restore`: an exact, externally pinned
  `PromotedOperationalGCSRestoreRequest`, or exactly `None`. There is no
  list/latest/nearest resolution anywhere -- a restore is either named
  explicitly by the operator or does not happen.

`promoted_operational_cloud_control.py` is pure: no filesystem, clock,
environment, network, or GCP/Kite access. It never reads the control file
itself -- that happens once, in `promoted_operational_cloud_job.py`, via
the already-accepted `read_stable_regular_file`.

## Fresh / restore / replay truth table

| Invocation | `prior_state_restore` | Effect |
|---|---|---|
| First run for a spec | absent | No restore call. Local stores start empty; the inner job computes a fresh terminal. |
| Restart, same durable state root | absent | No restore call. The inner job's own binding-reuse logic finds the already-durable local terminal and reuses it (no new quote acquisition). |
| Restart, fresh/ephemeral state root | present, pinned to the prior run's manifest coordinates | The wrapper restores the advisory/registration/terminal bundle into the local stores *before* the inner job runs; the inner job then reuses that restored terminal. |
| Restore request rejected (wrong generation/hash/spec/bucket, tampered artifact, local conflict) | present | The wrapper fails closed before ever invoking the inner job, the Kite factory, or state publication. |

Every restore is one externally pinned `restore_promoted_operational_state_from_gcs`
call, reusing the accepted revision-1/2/3 GCS-state boundary unchanged.

## One-client ordering

The wrapper constructs exactly one GCS client (via `gcs_client_factory`,
validated non-`None` before any adapter is built) and reuses that exact
object for every cloud-shaped capability in the invocation, in this order:

1. `GoogleCloudStorageObjectReader` / `GoogleCloudStorageStateObjectWriter`
   (both wrap the same client).
2. The optional restore (reader).
3. `india_swing.promoted_operational_job.main`, via
   `gcs_client_factory=lambda: shared_client` -- the inner job never
   constructs its own ambient/default client.
4. Final `publish_promoted_operational_state_to_gcs` (writer).

No second client is ever constructed. The inner job's own stdout/stderr
are captured in memory (never leaked to the wrapper's own stdout/stderr)
so the wrapper emits exactly one final envelope.

## Input-root prerequisite

**All thirteen path roots named in the control must already exist and be
populated inside the running container before this job is invoked.**
This increment performs no GCS hydration, no download, no read-only mount
setup, and no filesystem provisioning of any kind -- that wiring (staging
preparation/portfolio/reference inputs into the container from durable
storage) is explicitly a separate, later increment.

## Success pointer for the next invocation

On success, the wrapper does not trust the inner job's own stdout as the
state source. It resolves the terminal exactly once from its own local
`LocalPromotedOperationalTerminalStore`, cross-checks it against the inner
envelope, publishes the bundle, and emits one compact canonical JSON line
retaining the inner job's sanitized identifiers/status/action/failure
codes plus:

- `cloud_control_id`
- `state_publication_id`
- `state_manifest_object_name`
- `state_manifest_generation`
- `state_manifest_sha256`
- `state_manifest_byte_count`

These five `state_manifest_*`/`state_publication_id` fields are exactly
the coordinates an operator or orchestrator needs to build the *next*
invocation's `prior_state_restore` (a `PromotedOperationalGCSRestoreRequest`
pinned to this exact generation and hash), closing the restart loop.

## Non-goals

- No Cloud Run deployment, IAM, or Cloud Scheduler configuration.
- No live GCP call beyond what the injected `gcs_client_factory` and the
  already-accepted inner job perform -- every capability arrives only
  through injected seams in tests.
- No Telegram or other notification delivery.
- No interactive/browser login or Kite token refresh.
- No order placement, modification, or cancellation, and no capital
  authority of any kind.
- No bucket listing or "latest" object selection anywhere.
- No retry, repair, overwrite, or deletion of any published artifact.
- No GCS hydration of the input roots -- they must already exist.
