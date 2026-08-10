# Promoted operational hydrated cloud launch

Three new modules bridge a local promoted-operational input snapshot into
the already-accepted, Cloud Run-shaped `promoted_operational_cloud_job`
paper-trading job:

- `src/india_swing/promoted_operational_hydrated_cloud_control.py` --
  the pure, path-free `PromotedOperationalHydratedCloudLaunch` control
  type and its strict canonical codec.
- `src/india_swing/promoted_operational_input_publish_cli.py` -- the
  offline local publisher CLI
  (`india-swing-promoted-operational-input-publish`).
- `src/india_swing/promoted_operational_hydrated_cloud_job.py` -- the
  hydrated Cloud Run entrypoint
  (`india-swing-promoted-operational-hydrated-cloud-job`).

**This increment is paper-only integration.** It performs no deployment,
no scheduling, no Telegram delivery, no interactive/browser login, no
token refresh, and grants no real-capital authority.

The separately authorized outer paper-pilot boundary now composes this job
with durable, post-publication Telegram delivery. See
`docs/PROMOTED_PAPER_PILOT.md`. This hydrated job remains unchanged and has no
notification capability of its own.

## Offline-publish then hydrated-run flow

0. **Prepare the source control offline:** run
   `india-swing-promoted-operational-cloud-control-prepare` to load the exact
   assembly and local stores, derive the operational-run-spec ID through the
   accepted assembly boundary, and create the canonical source control. See
   `docs/PROMOTED_OPERATIONAL_CLOUD_CONTROL_PREPARE.md`. No cloud or broker
   capability is used in this step.
1. **Offline, on an operator's machine (or CI):** run
   `india-swing-promoted-operational-input-publish` with an existing,
   already-accepted `PromotedOperationalCloudRunControl` file
   (`--source-control-file`). It publishes the complete local input
   snapshot (the mandatory assembly-spec file plus the eleven read-only
   preparation/portfolio roots -- never `state_root`) to GCS exactly once,
   derives the exact externally pinned `PromotedOperationalInputRestoreRequest`
   from the resulting manifest, and writes one portable, path-free
   `PromotedOperationalHydratedCloudLaunch` launch-control file
   (`--output-launch-file`).
2. **Later, inside a fresh Cloud Run container:** run
   `india-swing-promoted-operational-hydrated-cloud-job` with
   `--launch-file` pointing at that same launch-control file (deployed
   alongside the container image or fetched by the surrounding
   infrastructure -- this increment does not define that transport). It
   acquires the complete pinned input snapshot, hydrates it into a fresh
   runtime directory beneath the container's own ephemeral filesystem,
   canonically derives and writes a local `PromotedOperationalCloudRunControl`
   bootstrap file, and invokes the already-accepted
   `promoted_operational_cloud_job.main` exactly once with the same
   shared injected GCS client.

## Trust boundaries

- The launch-control file is **path-free**: it carries only content
  identities (assembly-spec ID, operational-run-spec ID, target session,
  state bucket, the pinned input-restore request, and an optional pinned
  prior-state-restore request) and never a local filesystem path. It
  cannot direct a Cloud Run container's hydration to any destination
  other than the fixed runtime layout this module derives itself.
- Every nested restore request is reconstructed into a detached,
  independently re-verified instance before it is trusted, and every
  cross-lineage field (bucket, assembly-spec ID, operational-run-spec ID,
  target session) is cross-checked at construction.
- The publisher reads and decodes the source control file, then loads and
  cross-checks its assembly spec, **before** constructing any GCS
  capability. The hydrated job reads and decodes the launch file, then
  validates the runtime parent, **before** constructing any GCS, Kite, or
  runtime capability.
- Exactly one GCS client is constructed per invocation of either
  entrypoint. The hydrated job passes that same client into both
  `GoogleCloudStorageObjectReader` (for input-snapshot acquisition) and
  `promoted_operational_cloud_job.main`'s own `gcs_client_factory=lambda:
  client` seam -- it is never constructed a second time.
- The inner `promoted_operational_cloud_job` stdout envelope is untrusted
  even once it is canonical JSON with the exact accepted key set: every
  field is independently type/format checked, and every lineage field
  (assembly-spec ID, operational-run-spec ID, target session, and the
  derived cloud-control ID) is cross-checked against this job's own
  launch and hydration control before anything is forwarded into the
  hydrated job's own final stdout. `terminal_status` and `action` must be
  exact values of the accepted `PromotedOperationalRunStatus`/
  `PromotedOperationalDecisionAction` enums; `failure_codes` must be an
  exact, sorted, duplicate-free list drawn only from
  `PromotedOperationalRunFailureCode`, no longer than the full enum set;
  and the same COMPLETE/FAILED terminal-consistency rule the accepted
  terminal-record model itself enforces (COMPLETE carries no failure
  code; FAILED always carries at least one and is always paired with
  `NO_TRADE`) is re-verified here before anything is echoed.
  `state_manifest_object_name` must match the exact accepted promoted-
  operational-state manifest path shape, with its session, operational-
  run-spec-ID, and publication-ID path components cross-checked against
  the independently validated `target_session`, `operational_run_spec_id`,
  and `state_publication_id` -- arbitrary text in this field is never
  echoed. The captured inner stdout text is also bounded by a
  conservative maximum-byte ceiling, checked before any JSON parsing is
  attempted.
- The publisher's output-parent directory identity is captured with
  exactly one `lstat`-based `(st_dev, st_ino)` snapshot (never a split
  `Path.is_dir()` / symlink check, which is two separate syscalls) and
  rechecked before an existing-file replay read, immediately before
  exclusive creation, and after the cold read -- so a parent renamed away
  and replaced by a different directory at the same path between initial
  validation and the write is detected and fails closed, rather than
  reporting success merely because the replacement happens to contain
  byte-identical output. This is a bounded, observable-replacement
  discipline only: it makes no claim to eliminate an unobservable kernel-
  level race between a final check and the very next syscall.

## Fixed runtime layout

Production derives every runtime path beneath the fixed parent
`/tmp/india-swing`. That parent must already exist as an empty, safe,
non-link real directory; its filesystem identity is captured once and
rechecked before every later local write. Beneath it:

```text
/tmp/india-swing/assembly-spec.json         (hydrated input)
/tmp/india-swing/reference_root/            (hydrated input, one of eleven)
/tmp/india-swing/identity_evidence_root/    (hydrated input)
/tmp/india-swing/calendar_root/             (hydrated input)
/tmp/india-swing/daily_reports_root/        (hydrated input)
/tmp/india-swing/historical_corpus_root/    (hydrated input)
/tmp/india-swing/promoted_root/             (hydrated input)
/tmp/india-swing/graph_publication_root/    (hydrated input)
/tmp/india-swing/engine_run_root/           (hydrated input)
/tmp/india-swing/research_run_root/         (hydrated input)
/tmp/india-swing/operational_preparation_root/  (hydrated input)
/tmp/india-swing/portfolio_artifact_root/   (hydrated input)
/tmp/india-swing/state/                     (never part of the input snapshot)
/tmp/india-swing/runtime-control.json       (derived bootstrap file, written last)
```

The twelve hydrated-input destinations (the assembly-spec file plus the
eleven roots) are exactly the destinations the already-accepted
`hydrate_promoted_operational_input_snapshot` requires to be absent and
to share one common parent -- that parent is `/tmp/india-swing` itself.
`state/` and `runtime-control.json` are separate siblings that never
participate in that hydration boundary; `state/` is never statted,
created, or touched by any function in the hydrated-job module itself
(it is populated only later, by the inner cloud job's own local stores,
if at all).

For Windows unit tests only, `main()` accepts an injected concrete
absolute `runtime_parent` keyword argument in place of the fixed `/tmp`
path. This seam is **not** exposed as a CLI option -- a caller can never
redirect the runtime destination through `--launch-file` or any other
argument.

## Exact output/restart coordinates

- Publisher success stdout: one compact sorted JSON line with `status`,
  `launch_id`, `input_snapshot_id`, `expected_assembly_spec_id`,
  `expected_operational_run_spec_id`, `target_session`,
  `input_manifest_object_name`, `input_manifest_generation`,
  `input_manifest_sha256`, `input_manifest_byte_count`.
- Hydrated-job success stdout: one compact sorted JSON line that
  preserves every field of the accepted `promoted_operational_cloud_job`
  success envelope, plus `status`
  (`PROMOTED_OPERATIONAL_HYDRATED_CLOUD_JOB_COMPLETE`), `inner_status`
  (the inner job's own `PROMOTED_OPERATIONAL_JOB_COMPLETE`), `launch_id`,
  `input_snapshot_id`, `input_manifest_object_name`,
  `input_manifest_generation`, `input_manifest_sha256`, and
  `input_manifest_byte_count`. No bucket name, local path, credential, raw
  control/manifest byte payload, or nested exception ever appears in
  either envelope.

## Failure semantics

Both entrypoints are one bounded invocation each: no loop, retry, sleep,
polling, cleanup, or deletion exists in either. Failure produces no
stdout, exactly one static JSON stderr line
(`{"error_type": "...Error", "status": "FAILED"}`), and exit code 2.

The publisher's output file is create-once: an absent destination is
created exclusively and cold-read back byte-identical; a byte-identical
existing file is accepted idempotently; any other existing content
(different, a symlink/reparse point, non-regular, or a replaced parent)
fails closed. The publisher never overwrites, truncates, deletes, or
repairs its own output.

The hydrated job never writes directly into a final runtime destination
before it has been fully staged and verified (composing the accepted
`hydrate_promoted_operational_input_snapshot` boundary unchanged), and it
never writes `runtime-control.json` until hydration has independently
verified. Because the twelve hydration renames plus the
`runtime-control.json` write together are not one filesystem transaction,
any failure partway through -- including a fully completed, immutable GCS
upload from an *earlier* publish followed by a later local failure here
-- is an auditable failed attempt. It is never reported as success and
never automatically repaired; the caller/container must discard the
ephemeral runtime parent and start over.

## Non-goals

This increment does not: define how the launch-control file is
transported into a Cloud Run container image or mount; configure or
deploy any Cloud Run service, IAM binding, or Scheduler job; perform any
Telegram delivery, order placement, or other execution/notification
action (every restored/produced record remains permanently
`paper_only=True` / `notification_eligible=False` /
`execution_eligible=False` on its own already-accepted type); list a
bucket or select a "latest" object on any interface; retry, poll, or
clean up after a failure; or widen any existing byte/row/claim/risk
ceiling.
