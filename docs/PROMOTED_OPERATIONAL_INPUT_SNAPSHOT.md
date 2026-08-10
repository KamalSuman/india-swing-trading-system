# Promoted operational input snapshot

`src/india_swing/promoted_operational_input_snapshot.py` (pure models,
deterministic local inventory, canonical codec, destination-path mapping,
post-hydration re-verification) and `src/india_swing/promoted_operational_input_gcs.py`
(manifest-last GCS publication, exact generation-pinned acquisition, and
fail-closed local hydration) together are the immutable, generation-pinned
GCS input-snapshot foundation required to place the promoted paper engine
inside an ephemeral Cloud Run container.

**This increment performs no cloud-job wiring, no deployment, no live
GCP/Kite call, and no Telegram delivery.** It publishes and restores
exactly the read-only inputs a container needs before it can run
`promoted_operational_job`/`promoted_operational_cloud_job` -- it never
runs them itself.

## Snapshot contents

Exactly twelve fixed inputs, all already bound by
`PromotedOperationalCloudRunControl`:

- `assembly_spec_file` -- the mandatory assembly-spec file itself. Always
  represented by exactly one entry (`relative_path == ""`); a snapshot
  with no assembly-spec entry is never valid.
- Eleven read-only roots: `reference_root`, `identity_evidence_root`,
  `calendar_root`, `daily_reports_root`, `historical_corpus_root`,
  `promoted_root`, `graph_publication_root`, `engine_run_root`,
  `research_run_root`, `operational_preparation_root`,
  `portfolio_artifact_root`.

**`state_root` is categorically excluded from the snapshot.** Its path is
passed opaquely when the code reconstructs a required cloud-control object,
but it is never statted, listed, scanned, represented in an inventory or
manifest, published, acquired, written, or hydrated.

Every regular file under a root is included; any symlink, reparse point,
or unsupported filesystem entry type fails the whole scan closed. Every
included file, and the enclosing directory tree, is independently
re-verified unchanged across the full scan window before an inventory is
ever returned. An empty root is valid: a successful scan already proves
the root existed and was accessible, so zero entries under a root
unambiguously means "empty," never "unscanned." The inventory also binds
a canonical `input_names` tuple -- the exact twelve fixed names in fixed
order -- into its content identity and manifest bytes, so every root,
including one with zero file entries, is always explicitly represented in
the manifest rather than merely implied by absence of entries.

## Fixed ceilings (reused unchanged from the daily pipeline)

- 100,000 included files maximum.
- 256 MiB maximum per file.
- 2 GiB maximum total payload.
- 16 MiB maximum encoded manifest/inventory JSON.

None of these are widened here.

## Object layout

```text
promoted-operational-input/v1/{target_session}/{assembly_spec_id}/blobs/{sha256}
promoted-operational-input/v1/{target_session}/{assembly_spec_id}/manifests/{snapshot_id}.json
```

Both are fully derived -- never caller-selected, listed, searched, or
resolved by "latest" semantics. Content-identical files across different
inputs share exactly one blob object; publication writes at most one
`create_or_verify` call per unique blob.

## Manifest-last publication order

1. Build and independently re-verify the complete local inventory --
   **no writer capability is touched during this step.**
2. Write every unique blob, in canonical order, deduplicated by SHA-256.
3. Only if every blob write succeeds: construct, verify, and write the
   manifest last.

If any blob write fails, the manifest is never attempted -- a partial
publication may leave immutable orphan blob objects with no manifest
ever pointing at them; they are never deleted, repaired, or overwritten.

## Exact pinned restore flow

1. Read the exact manifest generation first, through the injected
   `GCSObjectReader`, verified against the *externally pinned* SHA-256
   (never a hash read from the object itself).
2. Decode the manifest; require its bucket/assembly-spec-id/session/
   snapshot-id and derived object name to agree with the pinned request.
3. Read every unique blob referenced by the manifest at its own pinned
   generation, independently re-verifying byte count and SHA-256 before
   trusting it.
4. Hold **all** acquired bytes in memory (bounded by the same 2 GiB total
   ceiling) -- nothing is written to any local destination until every
   blob has verified.
5. Hydrate: build the complete verified local layout inside one private,
   randomized staging directory beneath the twelve destinations' shared
   common parent, re-verify the staged tree with the same accepted
   inventory builder and assembly loader used elsewhere, and only then
   publish each destination by one same-parent rename. See "Empty-
   destination requirement" below for the exact contract.
6. Independently rebuild the inventory from the hydrated destinations and
   require it to match the acquired manifest byte-for-byte; reload the
   hydrated assembly spec through the accepted loader and recheck its
   identity/session/bucket against the control before returning a
   completed restoration.

No bucket listing or "latest" object selection exists on any interface in
either module.

## Empty-destination requirement

Hydration targets a fresh ephemeral filesystem (a Cloud Run container's
own writable layer). Before any local write:

- **All twelve destinations** -- the assembly-spec file and all eleven
  roots -- **must be exactly absent.** Unlike an earlier design, an
  existing *empty* root directory is no longer accepted; any pre-existing
  entry at any destination, even an empty directory, fails the whole
  hydration closed before a single byte is written.
- All twelve destinations must be **direct children of one existing,
  verified, non-root, non-link common parent directory.** That parent's
  exact filesystem identity (device/inode) is captured once and rechecked
  immediately before creating the staging directory, immediately after
  creating it and before any staged content is written, and again
  immediately before each of the twelve final renames.

Content is never written directly into a final destination. Instead, the
complete verified layout is first built inside one private, randomized
staging directory created (via `tempfile.mkdtemp`) directly beneath the
same verified common parent. The staging directory's own identity is
likewise captured once and rechecked before and after the staged tree is
created, before staged-tree verification, and before the first rename.
The staged tree is then re-verified with the same accepted
`build_promoted_operational_input_inventory`/
`load_promoted_operational_assembly_spec_file` functions used everywhere
else in this module -- not a separate, duplicated check. Only after that
re-verification succeeds does publication proceed: each of the twelve
destinations is published by exactly one same-parent `os.rename` from the
staging directory, re-checking the common parent's identity and that
specific destination's absence immediately before its own rename.

This bounded identity discipline defends against an observable
replacement of the common parent or staging directory between checks; it
makes no claim to protect against an unobservable kernel-level race
between a check and the very next syscall.

## Partial-write / partial-rename discard rule

Individual file writes inside the staging directory use exclusive/atomic
local create semantics (`O_CREAT | O_EXCL`) with fstat-based single-
hard-link and regular-file identity verification, an exact write-length
check, and an explicit flush/fsync -- a staged file is never overwritten.

Because twelve top-level renames cannot be one filesystem transaction, a
failure during or after staging -- including partway through the rename
sequence -- is **never repaired, rolled back, cleaned up, or retried**.
Any state left behind (a partially populated staging directory, or a
destination layout with only some of the twelve renames completed)
remains unusable and reports no completed result; the caller/container
must discard the entire ephemeral common parent and start over rather
than repair in place. `state_root` is never statted, created, staged, or
renamed by any function in this module, and never participates in
common-parent or staging-directory identity.

## Later cloud-job integration (not part of this increment)

A future, separately reviewed increment will wire this snapshot boundary
into `promoted_operational_cloud_job.py`'s existing one-shared-client
contract: the same `StateObjectWriter`/`GCSObjectReader` pair the cloud
job already constructs from one `gcs_client_factory()` call would be
reused for input-snapshot publication/acquisition too, before the inner
job runs. This increment defines the primitives only; it performs no such
wiring, no deployment, and no live call.
