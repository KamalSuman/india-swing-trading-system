# Promoted operational GCS publication and restoration

`src/india_swing/promoted_operational_gcs_state.py` is the immutable,
generation-pinned GCS publication and restoration boundary for one
already-verified `PromotedOperationalTerminalRecord`. It is modeled on
`src/india_swing/operations/gcs_state.py`, adapted to compose only the
accepted promoted-operational record types
(`PromotedOperationalTerminalRecord` / `PromotedOperationalAdvisoryRecord` /
`PaperTradeRegistration`), their existing strict codecs, and
`verify_promoted_operational_published_bundle`. It never reproduces any
strategy, quote, allocation, risk, sizing, terminal-binding, or
paper-registration logic, and it never modifies an existing accepted module.

**This increment performs no Cloud Run deployment, no live GCP call, no
Telegram delivery, no quote acquisition, and no order execution.** It is
paper-only durability: every published/restored terminal and advisory
record remains `paper_only=True` and permanently
`notification_eligible=False` / `execution_eligible=False`.

## Object layout

All object names are derived, never caller-selected, listed, searched,
globbed, or chosen by latest/nearest semantics:

```text
promoted-operational-state/v1/{target_session}/{spec_id}/artifacts/{kind}/{content_id}.json
promoted-operational-state/v1/{target_session}/{spec_id}/manifests/{publication_id}.json
```

`{kind}` is one of `advisory`, `paper_registration`, `terminal` (the
lowercase value of `PromotedOperationalGCSArtifactKind`, in that fixed
canonical order). `{content_id}` is the artifact's own already-accepted
content-addressed identity: `advisory_id`, `registration_id`, or
`terminal_id` respectively -- never an opaque or caller-chosen name.

A terminal's artifact set is exactly `(ADVISORY, TERMINAL)`, or
`(ADVISORY, PAPER_REGISTRATION, TERMINAL)` when
`terminal.paper_registration_id is not None`. There is no other shape.

## Manifest

`PromotedOperationalGCSManifest` binds: schema version, bucket, `spec_id`,
`preparation_id`, `target_session`, `terminal_id`, the terminal's own
`status`/`action`, `advisory_id`, an optional `paper_registration_id`, the
exact canonical artifact tuple, and a deterministic `publication_id`
(a `content_id` hash over every other field). It is encoded as compact
sorted canonical JSON with a trailing newline and decoded with strict
UTF-8, duplicate-key rejection at every level, exact key sets, no
floats/NaN/Infinity, exact enum/dataclass types, a final byte-for-byte
canonical re-encode check, and a 256 KiB ceiling.

Per-artifact ceilings are conservative and immutable: advisory 512 KiB,
terminal 512 KiB, paper registration 1 MiB -- none of these widen the
tighter ceilings the underlying accepted advisory/terminal/registration
codecs already enforce; they are this layer's own outer bound.

## Publication order

`publish_promoted_operational_state_to_gcs` accepts exactly
`PromotedOperationalTerminalRecord`, `bucket`, `StateObjectWriter`,
`LocalPromotedOperationalAdvisoryOutbox`, and `LocalPaperTradeLedger`. It
resolves the advisory **only** by `terminal.advisory_id` and the optional
registration **only** by `terminal.paper_registration_id` -- never a
caller-supplied value -- and calls
`verify_promoted_operational_published_bundle` before any writer call.

Writes happen in canonical dependency order, one `StateObjectWriter.create_or_verify`
call per artifact with the exact derived object name / `application/json`
content type / per-kind ceiling, and the manifest is written **last**:

1. advisory
2. paper registration (only when present)
3. terminal
4. manifest (terminal-last publication boundary)

Every returned `PublishedStateObject` is independently re-verified (exact
type, object name, byte count, SHA-256) before being trusted; a malformed
or malicious writer return fails the whole publication closed.

A partial crash may leave immutable orphan GCS artifacts (an advisory or
terminal object with no manifest ever pointing at it) -- these are never
deleted, repaired, listed, or overwritten. Without a sealed manifest,
`restore_promoted_operational_state_from_gcs` has nothing to pin, so orphan
artifacts are simply unreachable, not corrupting state.

## Pinned restore contract

`PromotedOperationalGCSRestoreRequest` externally pins `bucket`, the exact
manifest `object_name`, a positive non-bool `generation`, the expected
manifest SHA-256, and the expected `spec_id`. The manifest object name
must itself encode and agree with the expected `spec_id` (and a valid
`target_session` date) -- no internally stored hash may attest to itself.

`restore_promoted_operational_state_from_gcs` uses only injected
`GCSObjectReader.read_generation` calls, never a listing or "latest"
lookup:

1. Read the exact manifest generation first; verify generation, type,
   non-empty/ceiling bound, and the *externally pinned* SHA-256 (never a
   value read from the object itself).
2. Strictly decode the manifest; require `bucket`/`spec_id`/derived path
   agreement with the pinned request.
3. Read exactly the manifest's own canonical artifact tuple, each by its
   own pinned generation, independently re-verifying byte count and
   SHA-256 before decoding.
4. Call `verify_promoted_operational_published_bundle` again on the
   decoded terminal/advisory/registration, then cross-check every decoded
   value against the manifest's own retained fields.
5. Restore to the existing `LocalPromotedOperationalAdvisoryOutbox` and
   `LocalPaperTradeLedger` **first**, then write
   `LocalPromotedOperationalTerminalStore` **last**.

If any read, decode, identity, or cross-link check fails -- or either
local parent-store write fails -- the terminal store is never touched.

## Crash / replay truth table

| Crash point | Effect on retry |
|---|---|
| Before the manifest is ever written | Nothing to restore; orphan GCS artifacts (if any) are simply unreachable. |
| After the manifest, before any local restore | Restore starts clean; advisory/registration/terminal are all written fresh. |
| After advisory/registration restored locally, before the terminal write | Local advisory/registration writes are create-once and idempotent; retry safely re-derives and writes only the still-missing terminal record. |
| Full byte-identical replay | `create_or_verify`/local-store `put` are idempotent for identical content; restore returns the same terminal. |
| Conflicting pre-existing local state at the same key | Fails closed with a static sanitized error; never repaired, deleted, or silently overwritten. |

## Sanitized errors

Every boundary failure surfaces as one static `PromotedOperationalGCSStateError`
with `__cause__ is None` and `__context__ is None` -- no chained exception,
no underlying exception text/repr, no bucket, path, ID, hash, credential,
quote, or portfolio value ever appears in the message. This is implemented
with the deferred-flag idiom (set a boolean flag inside an `except` block,
raise the static message *after* leaving the `try`/`except` entirely) so
Python's automatic `__context__` population -- which `raise ... from None`
alone does not suppress -- never occurs.

## Non-goals

- No Cloud Run deployment, IAM, or Cloud Scheduler configuration.
- No live GCP call: every cloud-shaped capability arrives only through an
  injected `StateObjectWriter`/`GCSObjectReader` port.
- No Telegram or other notification delivery.
- No quote acquisition, portfolio acquisition, allocation, or decision
  computation -- this module only publishes/restores an already-produced
  terminal/advisory/registration bundle.
- No order placement, modification, or cancellation, and no capital
  authority of any kind.
- No environment variable, wall-clock, or ambient/default GCP client
  access; no bucket listing or "latest" object selection anywhere.
