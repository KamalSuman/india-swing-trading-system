# Promoted terminal-binding control plane

`src/india_swing/promoted_terminal_binding.py` and
`src/india_swing/promoted_terminal_binding_control_plane.py` implement the
independent trusted-terminal-binding retention that
`TrustedPromotedOperationalTerminalBinding` (in
`promoted_operational_service.py`) requires but deliberately does not
provide on its own: a durable `spec_id -> expected_terminal_id` anchor,
sealed to GCS through the existing write-side abstractions, and readable
back at restart from nothing but the live run spec.
`src/india_swing/promoted_operational_anchored_session.py` (see
[Anchored session](#anchored-session) below) now joins that control plane
to the local publication service behind one schedulable entry point.
Neither of these modules constructs a real storage client, and no
deployment, Cloud Run/Scheduler, or Telegram capability exists anywhere
here -- those remain later, separately authorized increments.

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
| No local terminal, no sealed binding | Normal starting state; `run_publish_and_anchor_promoted_operational_session` runs the pipeline once and seals the binding after terminal-last publication. |
| Local terminal created, binding not yet sealed (crash in between, or the seal write itself fails) | **Deliberately fail-closed, manual-recovery.** Nothing in this codebase infers, backfills, or reconstructs a binding from the local terminal. Every subsequent call with the same spec fails closed (the local terminal now demands a binding that still doesn't exist) rather than rerunning the market pipeline; the local advisory, paper registration, and terminal are never deleted, overwritten, or repaired. |
| Binding sealed, local terminal present and matches | The intended steady state: `run_publish_and_anchor_promoted_operational_session` replays without touching the clock, either source, or the control plane's write path. |
| Binding sealed for spec X, a *different* terminal later attempted for the same spec X | The writer conflict check rejects it; the originally sealed binding is never overwritten. This includes the race where a concurrent seal becomes visible between this session's own absence-proving read and its own seal attempt -- the conflict is still caught at write time. |
| Binding object exists remotely but is corrupted, truncated, or oversized | `load_trusted_promoted_operational_terminal_binding`/`load_optional_trusted_promoted_operational_terminal_binding` fail closed with a sanitized error; neither attempts to reconstruct or repair the object, and corruption is never treated as absence. |
| Binding sealed for spec X, but no local terminal exists for X in this store | `run_and_publish_promoted_operational_service` fails closed before any source, acquisition, clock, advisory, or ledger access (an anchored terminal that isn't locally present is itself an inconsistency). |
| The `binding_bucket` is misconfigured or unreachable | The bucket-level preflight fails closed with `PromotedOperationalControlPlaneUnreachableError` before the binding load, before any market acquisition, and before any local write -- so a configuration mistake never produces a local terminal, never touches the market, and never reaches manual-recovery state. |

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

## Anchored session

`src/india_swing/promoted_operational_anchored_session.py` closes the
restart gap end to end: `run_publish_and_anchor_promoted_operational_session(
*, spec, quote_source, portfolio_source, clock, advisory_outbox,
terminal_store, paper_ledger=None, binding_bucket, binding_writer,
binding_reader)` is one thin orchestrator that routes solely on the
**remote anchor** and delegates every local-terminal/binding decision to
the already-accepted `run_and_publish_promoted_operational_service`, which
alone implements that complete truth table. It never reads `terminal_store`
itself, never branches on local terminal presence, never touches the clock
or either source directly, and never constructs a
`TrustedPromotedOperationalTerminalBinding` itself.

### The optional read

`PromotedTerminalBindingObjectReader` gained a second method,
`read_current_optional(*, bucket, object_name, maximum_bytes) ->
ObservedTerminalBindingObject | None`, alongside the existing (byte-for-byte
unchanged in observable behavior) `read_current`. Both share the identical
observe-then-pin-then-verify sequence via one private helper on
`GoogleCloudStorageTerminalBindingReader`, so the two paths can never
drift. `read_current_optional` returns `None` **only** when the exact
object is proven not to exist; every other failure -- malformed content,
an oversized payload, a permission failure, a generation change between
observation and download, or any other error -- still raises the
sanitized `PromotedTerminalBindingControlPlaneError`.

`load_optional_trusted_promoted_operational_terminal_binding(*, spec,
bucket, reader)` mirrors `load_trusted_promoted_operational_terminal_binding`
exactly (both call one shared private verification helper so the two
paths can never drift) except it returns `None` only when
`reader.read_current_optional` itself proved absence.

### Proven-absence rule

*Absence is a safety boundary, not a convenience.* `load_trusted_...`
already has no optional variant because absence and corruption must never
be conflated -- "no anchor sealed yet" is the normal first-run case, while
"anchor unreadable" must never be allowed to start a fresh market run.
Absence is therefore proven **only** by the storage layer's own exact
not-found signal (an `isinstance` match against `google.api_core.exceptions.NotFound`)
and by nothing else -- never an empty payload, a falsy/`None` generation,
an `AttributeError`, a `KeyError`, a `LookupError`, or any message text.

### Environment consequence: the SDK is absent here

`google-cloud-storage` and `google.api_core` are both uninstalled in this
environment, verified this session. `promoted_terminal_binding_control_plane.py`
follows the exact pattern `state_publication.py` already establishes:
import the SDK exception type inside `try/except ImportError`, bind `None`
on failure, and guard every use with `if NotFound is not None and
isinstance(error, NotFound)`. The consequence is deliberate and preserved
here: when the not-found type is unavailable, `read_current_optional` can
never report absence and fails closed for every error instead, including
an exact missing-object condition. Absence is therefore exercised in this
codebase's own tests through fake readers at the port level, never by
pretending the adapter mapped a real `NotFound` it cannot currently
construct.

### Bucket-level preflight: why parsing the object-level NotFound was rejected

GCS reports a missing bucket with the *exact same* `NotFound` exception as
a missing object, and `read_current_optional` deliberately refuses to
inspect exception message text to tell them apart (message text is not a
stable, sanitizable signal). Left alone, that ambiguity meant a
misconfigured `binding_bucket` read as proven absence: the anchored
session would proceed to acquire real market data, publish local
advisory/registration/terminal artifacts, and only then fail at the seal
-- leaving that spec permanently in manual recovery for what was purely a
configuration mistake.

The fix is not to parse the error further; it is to ask a **bucket-level**
question, whose not-found answer is unambiguous by construction (a bucket
either exists or it doesn't -- there is no sibling "object inside a valid
bucket" case to confuse it with). `PromotedTerminalBindingControlPlanePreflight.verify_bucket_reachable(
*, bucket)` does exactly that: `GoogleCloudStorageTerminalBindingReader`
implements it with exactly one call, `client.get_bucket(bucket)`,
requiring the returned object's `name` to equal the requested bucket
before returning. It never calls `blob()`, `download_as_bytes`,
`upload_from_string`, `reload` on a blob, or any listing method -- it
asks a bucket-level question and nothing else. Any exception, a
missing/non-string `name`, or a `name` mismatch all collapse into the
same sanitized `PromotedTerminalBindingControlPlaneError`, exactly like
every other failure in this module.

### Typed errors on the anchored session

`run_publish_and_anchor_promoted_operational_session` classifies exactly
the three outcomes this orchestration layer owns directly -- never by
inspecting local terminal state, and never by inspecting exception message
text:

| Type | Signals |
|---|---|
| `PromotedOperationalControlPlaneUnreachableError` | The bucket-level preflight call itself failed. Raised before the binding load, before any market acquisition, and before any local write. |
| `PromotedOperationalBindingUnreadableError` | `load_optional_trusted_promoted_operational_terminal_binding` failed for any non-absence reason (corruption, truncation, oversize, a generation change, or any other error). A proven-absent binding is not this error -- it routes normally as `terminal_binding=None`. |
| `PromotedOperationalManualRecoveryRequiredError` | A genuinely fresh local terminal was published (`reused_existing_terminal is False`) but `seal_promoted_operational_terminal_binding` then failed. This is the one path the anchored session knows for certain has left a local terminal published with no sealed binding -- the deliberate manual-recovery state. Its existing behavior is unchanged: no delete, no overwrite, no retry, no repair. |

All three are subclasses of the unchanged `PromotedOperationalAnchoredSessionError`,
so existing callers and tests that catch the base type keep working. **A
failure raised by `run_and_publish_promoted_operational_service` itself
stays the plain, unclassified base type.** Distinguishing its internal
causes (a local terminal exists but no binding was supplied; a binding
exists but no local terminal does; a replay cross-check disagreed) would
require this layer to start reading or inferring local terminal state
itself -- which would break the routing rule that the remote anchor is the
only routing input. None of the four typed classes -- base or the three
subclasses -- ever includes a bucket name, an object name, a generation, a
hash, a spec/terminal ID, payload bytes, or nested exception text; every
raise uses a short static message and the established flag-then-raise-
after-the-try-block pattern so `__cause__` and `__context__` are both
`None` on every path.

### Exact call order

1. Validate exact types and `binding_bucket` first.
2. Call `binding_preflight.verify_bucket_reachable(bucket=binding_bucket)`
   exactly once -- before anything else, including the binding load, any
   market acquisition, any local write, and any seal. Any failure raises
   `PromotedOperationalControlPlaneUnreachableError` immediately, with
   nothing else attempted.
3. Call `load_optional_trusted_promoted_operational_terminal_binding`
   exactly once. Its result -- and nothing else -- becomes the
   `terminal_binding` argument below. A non-absence failure here raises
   `PromotedOperationalBindingUnreadableError`.
4. Call `run_and_publish_promoted_operational_service` exactly once with
   the caller's exact `spec`, sources, clock, stores, ledger, and that
   binding.
5. If and only if the returned state has `reused_existing_terminal is
   False`, call `seal_promoted_operational_terminal_binding` exactly once
   with `terminal=<the exact terminal the service just returned>` -- never
   a value re-read from `terminal_store`. A seal failure here raises
   `PromotedOperationalManualRecoveryRequiredError`.
6. On replay (`reused_existing_terminal is True`), no seal call happens at
   all; instead the already-loaded binding's `expected_terminal_id` and
   `spec_id` are re-checked against the returned terminal at this
   boundary, failing closed on any disagreement even though the service
   already enforced it.

## Non-goals

- No real storage client is constructed anywhere; `GoogleCloudStorageTerminalBindingReader`
  requires an already-constructed client injected by its caller.
- No deployment, Cloud Run, Cloud Scheduler, or job configuration exists
  here.
- No Telegram, notification, or execution capability exists here.
- No automatic recovery, repair, backfill, or local-terminal-derived
  binding exists here -- a local terminal without a sealed binding stays
  fail-closed and manual-recovery, by design, and a seal failure after a
  fresh publication never deletes, overwrites, retries, or repairs the
  local advisory, paper registration, or terminal record that was already
  published.
