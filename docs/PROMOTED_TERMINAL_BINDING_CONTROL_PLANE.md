# Promoted terminal-binding control plane

`src/india_swing/promoted_terminal_binding.py` and
`src/india_swing/promoted_terminal_binding_control_plane.py` implement the
independent trusted-terminal-binding retention that
`TrustedPromotedOperationalTerminalBinding` (in
`promoted_operational_service.py`) requires but deliberately does not
provide on its own: a durable `spec_id -> expected_terminal_id` anchor,
sealed to GCS through the existing write-side abstractions, and readable
back at restart from nothing but the live run spec. This increment
delivers the artifact, its codec, the read/write ports, and the seal/load
functions only. It does **not** wire the control plane into
`run_and_publish_promoted_operational_service`, construct a real storage
client, or add any deployment, Cloud Run/Scheduler, or Telegram capability
-- those are later, separately authorized increments.

## Record schema

`PromotedOperationalTerminalBindingRecord` is frozen/slots and
content-addressed:

| Field | Type | Source |
|---|---|---|
| `schema_version` | `str` | fixed `promoted-operational-terminal-binding/v1` |
| `spec_id` | `str` (lowercase SHA-256) | `spec.spec_id` |
| `target_session` | `date` | `spec.quote_gate_spec.preparation.manifest.target_session` |
| `preparation_id` | `str` (lowercase SHA-256) | `spec.quote_gate_spec.preparation.manifest.preparation_id` |
| `expected_terminal_id` | `str` (lowercase SHA-256) | `terminal.terminal_id` |
| `terminal_completed_at` | `datetime` (tz-aware, UTC offset) | `terminal.completed_at` |
| `binding_id` | `str` (`field(init=False)`) | `content_id(...)` over every other field |

`build_promoted_operational_terminal_binding_record(terminal, spec)` is the
only way to construct one from real inputs: it requires exact types, calls
`terminal.verify_content_identity()`, `spec.verify_content_identity()`, and
`_verify_terminal_matches_spec(terminal, spec)` (imported unchanged from
`promoted_operational_persistence.py`), and then derives every field only
from retained values. It never reads a clock and never accepts a
caller-supplied `binding_id`.

## The exact spec-derived object name

```text
promoted-operational/terminal-bindings/{target_session.isoformat()}/{spec_id}.json
```

`promoted_operational_terminal_binding_object_name(spec)` is the **only**
function in either module that derives this path, and it takes a live,
verified `PromotedOperationalRunSpec` -- never a terminal record, a
binding record, or a free-form string. This is deliberate: the read
path's safety depends on the path being fixed by the caller's live spec,
not by anything stored. A stored record's own `target_session` field is
never consulted to compute where to read from.

## Reused write port: why no new writer exists

`StateObjectWriter.create_or_verify` (in `daily_pipeline/state_publication.py`)
already provides exactly what sealing needs: `if_generation_match=0`
create-once upload, and on a `PreconditionFailed` conflict, a reload to
observe the existing generation, a pinned re-download, and a byte-for-byte
equality check against the supplied content before returning. That is
create-once plus conflict detection plus idempotent replay. This task
reuses `StateObjectWriter`/`PublishedStateObject`/
`GoogleCloudStorageStateObjectWriter` completely unchanged -- no second
writer, no second `PublishedStateObject`, and no second upload path exist
anywhere in this module.

## New read port: why generation-pinned `read_generation` was insufficient

Every existing `GCSObjectReader.read_generation` call in this codebase is
pinned to a generation the caller already retained (see
`PinnedStatePublicationRequest` and
`acquire_verified_pipeline_state_control`). The terminal binding cannot
use that shape: at restart, the caller knows only the run spec -- there is
no generation to supply yet. `PromotedTerminalBindingObjectReader.read_current(
*, bucket, object_name, maximum_bytes)` is therefore one small, new read
port that:

1. Observes the **current** generation of exactly one deterministic
   object name (`blob.reload()`), never a bucket listing and never a
   "latest object" selection.
2. Re-pins that exact generation on a second handle and downloads with
   `raw_download=True, if_generation_match=<observed>`.
3. Re-reads the pinned handle's own `.generation` and requires it to still
   equal the observed value, rejecting any change between observation and
   download.
4. Computes the SHA-256 of the downloaded bytes locally rather than
   trusting any client-reported hash.

`GoogleCloudStorageTerminalBindingReader`'s constructor requires an
already-constructed client -- it never imports `google.cloud.storage`,
never constructs a client, never reads an environment variable or
credential, and never falls back to an ambient default. The deployment
caller constructs and injects the real client in a later, separately
authorized increment.

## Create-once and conflict semantics

`seal_promoted_operational_terminal_binding(*, terminal, spec, bucket,
writer)` calls `writer.create_or_verify` exactly once, then independently
re-verifies the returned `PublishedStateObject` (exact type, object name,
byte count, SHA-256) before returning
`SealedPromotedOperationalTerminalBinding(record, bucket, published)`. A
writer conflict against different pre-existing bytes -- for example, a
second terminal for the same `spec_id` (and therefore the same object
name) producing a different `expected_terminal_id` -- propagates as a
sanitized `PromotedTerminalBindingControlPlaneError` and is never retried,
overwritten, deleted, or repaired. An identical reseal (byte-for-byte the
same record) is idempotent and returns the same published object.

## Seal/load call order

- **Seal**: build the record -> encode it -> derive the object name from
  the spec -> `writer.create_or_verify` -> independently re-verify the
  result -> construct and return `SealedPromotedOperationalTerminalBinding`.
- **Load**: derive the object name from the spec -> `reader.read_current`
  exactly once -> independently re-verify the `ObservedTerminalBindingObject`
  -> decode the record strictly -> project it through
  `trusted_binding_from_record(record, spec)` (which itself re-checks
  `spec_id`/`target_session`/`preparation_id` against the live spec) ->
  construct and return `LoadedPromotedOperationalTerminalBinding`.

There is no optional/`None` variant of `load_trusted_promoted_operational_terminal_binding`:
absence, permission failure, malformed content, an oversized payload, a
generation change between observation and download, or any spec mismatch
all raise one sanitized `PromotedTerminalBindingControlPlaneError`.
Absence is never treated as a benign case.

## Crash/restart matrix

| State | Behavior |
|---|---|
| No local terminal, no sealed binding | Normal starting state. |
| Local terminal created, binding not yet sealed (crash in between) | **Deliberately fail-closed, manual-recovery.** Nothing in this task infers, backfills, or reconstructs a binding from the local terminal. The binding must be sealed from a fresh, independently-verified terminal/spec pair, or the state is repaired by a human operator. |
| Binding sealed, local terminal present and matches | The intended steady state: a caller can later load the binding and pass it as revision 3's `terminal_binding` (not wired in this increment). |
| Binding sealed for spec X, a *different* terminal later attempted for the same spec X | The writer conflict check rejects it; the originally sealed binding is never overwritten. |
| Binding object exists remotely but is corrupted, truncated, or oversized | `load_trusted_promoted_operational_terminal_binding` fails closed with a sanitized error; it never attempts to reconstruct or repair the object. |

## Honest trust model

This control plane's authority comes from **create-once object
immutability plus independently observed generation pinning**, not from
content hashing. A self-consistent forged binding whose `binding_id`
recomputes correctly and whose spec cross-checks pass will be accepted by
the load path -- that is inherent to any content-addressed record and is
not papered over here. The real defense is structural: a binding object
for a given spec can never be replaced once sealed. A conflicting seal
fails closed. Any party who can overwrite that object at a new generation
**is** the trust root for this scheme and is outside this task's threat
model. Neither module claims cryptographic authentication or independent
provenance anywhere in code, comments, or this document.

## Non-goals

- `run_and_publish_promoted_operational_service` is not modified and does
  not call any function in this module -- wiring the control plane into
  the service is a later, separately authorized increment.
- No real storage client is constructed anywhere; `GoogleCloudStorageTerminalBindingReader`
  requires an already-constructed client injected by its caller.
- No deployment, Cloud Run, Cloud Scheduler, or job configuration exists
  here.
- No Telegram, notification, or execution capability exists here.
- No automatic recovery, repair, backfill, or local-terminal-derived
  binding exists here -- a local terminal without a sealed binding stays
  fail-closed and manual-recovery, by design.
