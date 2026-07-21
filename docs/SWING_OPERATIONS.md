# Operational swing-run boundary

The operational package composes the deterministic decision engine into one
paper-only daily run without granting order authority. Its public service is
`execute_swing_operational_run`.

## Immutable run specification

`build_swing_operational_run_spec` binds:

- the exact content-addressed `SwingProposalBatch`;
- its next-session entry window and target session;
- quote, ranking, and INR portfolio-sizing policies;
- a maximum quote chunk size no larger than Kite's 500-symbol request limit;
- one-new-position-per-run policy; and
- `PAPER_ONLY` authority.

The same specification has the same ID. A local operational store permits only
one terminal record for that ID, so an accidental rerun cannot produce a second
different recommendation for the same scheduled decision.

## Broad-universe quote acquisition

Kite's per-request full-quote limit remains 500 symbols. The operational runner
sorts the complete proposal key set, requests deterministic chunks, validates
each chunk independently, and aggregates them into one content-addressed quote
snapshot supporting up to 10,000 keys. The decision quote gate now maps sorted
quote keys back to stable-ID-ordered proposals rather than assuming the two
orders happen to match.

The aggregate records the earliest request time and latest observation time.
The normal quote-gate collection-duration policy therefore still rejects a scan
that took too long; chunking cannot hide stale cross-sectional collection.

## Injected, read-only sources

The runner accepts `SwingQuoteSource`, `SwingPortfolioSource`, and an injected
clock. `KiteSwingQuoteSource` exposes only the existing read-only
`fetch_full_quotes` capability. `FixedSwingPortfolioSource` supports a manually
supplied, immutable portfolio snapshot without reading credentials or a broker.

A real account adapter is intentionally not guessed from Kite margins and
holdings: cash and gross exposure can be read from a broker, but current open
risk also requires the engine's own accepted-position and stop ledger. That
reconciler is the next account-integration boundary.

## Fail-closed results

Every acquired-input or timing failure becomes an immutable `FAILED` /
`NO_TRADE` result with only sanitized codes:

- start before or after the entry window;
- quote acquisition failure;
- malformed/incomplete quote coverage;
- portfolio acquisition failure;
- non-monotonic clock;
- evaluation or completion after the entry deadline; or
- decision assembly failure.

Upstream exception text is never included. A failed result has no decision
package and no paper registration, but preserves any valid quote or portfolio
input acquired before the failure for audit identity.

## Publication ordering and paper ledger

`publish_swing_operational_run` performs idempotent side effects in this order:

1. publish the verified decision notification;
2. register a BUY in the append-only paper ledger; and
3. publish the terminal operational manifest last.

If a side effect fails, no terminal manifest is written. A rerun can safely
finish the same idempotent handoffs. A NO_TRADE decision publishes its
notification but no paper registration. A failed operational run publishes only
its warning-bearing terminal record.

`publish_operational_record_to_gcs` publishes the same canonical record under
`operational/YYYY-MM-DD/<spec-id>.json` through the existing hardened
`StateObjectWriter`. The production Google Cloud Storage writer uses a
create-only generation precondition and, on conflict, reads the exact pinned
generation back to verify identical bytes. The operational adapter independently
checks path, length, and SHA-256 before accepting the publication receipt.

Paper registrations may now be created after the entry window opens, as required
by a live quote gate, but they must still predate entry expiry. Paper events are
independently forbidden from predating the decision timestamp, preventing a
backdated simulated fill.

## Inspection CLI

Published manifests can be inspected without credentials:

```powershell
india-swing-operational --root var/operational list
india-swing-operational --root var/operational show --spec-id <sha256>
```

The CLI does not run Kite or deserialize arbitrary proposal objects. Scheduling
calls the typed operational service with an exact stored proposal-batch ID and
a typed resolver for its pinned parent artifacts.

## Proposal artifact boundary

Operational runs no longer require an in-process caller to hand them an
unrecorded proposal object. `LocalSwingProposalBatchStore` persists a strict,
create-once replay manifest keyed by the exact `proposal_batch_id`. The
manifest binds the universe batch, current universe, calendar, signal policy,
assemblies, proposals, vetoes, session, cutoff, and coverage counts.

The manifest is intentionally not a generic serialization of the historical
input graph. `build_stored_swing_operational_run_spec` resolves the three exact
typed parents, verifies their content identities, deterministically rebuilds
the full proposal batch, and compares every recorded identity before creating
an operational spec. There is no `latest` selection, pickle/object loading, or
ability for manifest content to grant itself trading authority.

`LocalSwingProposalParentStore` makes that resolver restart-safe. It stores
only three approved root types under their exact IDs: `SwingUniverseInputBatch`,
`CalendarSnapshot`, and `DeterministicSwingSignalConfig`. Its decoder is driven
by those roots' declared field types; it cannot import a type named by the file,
execute code, or instantiate an unapproved root. Every computed nested ID is
recomputed during reconstruction, and the canonical bytes are regenerated and
compared before a read is accepted.

`publish_swing_proposal_with_parents` writes the universe batch, calendar, and
signal policy first, verifies that a fresh exact-ID replay reproduces the
proposal batch, and writes the small terminal proposal manifest last. Partial
parent publication is harmless because parent objects are immutable and
content-addressed; a schedulable proposal is not visible until its terminal
manifest exists.

`run_and_publish_stored_swing_operation` is the schedulable service boundary:
exact proposal ID load, quote and portfolio acquisition, decision assembly,
paper registration, notification publication, and terminal record sealing.
It remains `PAPER_ONLY` and has no broker-order capability.

The upstream graph can now be prepared without an in-process proposal object.
`india-swing-proposal-prepare` loads an exact stored universe batch, calendar,
and signal policy by ID, replays complete actionable/veto coverage and every
promotion-decision binding, then publishes the proposal manifest terminal-last.
See `docs/PROPOSAL_PREPARATION.md`.

Manifest inspection is available without loading market histories:

```powershell
india-swing-operational --proposal-root var/proposals proposal-list
india-swing-operational --proposal-root var/proposals proposal-show --proposal-batch-id <sha256>
india-swing-operational --proposal-root var/proposals --parent-root var/proposals proposal-verify --proposal-batch-id <sha256>
```

`proposal-verify` performs a full fresh-store parent decode and deterministic
proposal replay, but it does not request quotes, read an account, or publish a
decision.

## Scheduled operational job

`india-swing-operational-job` is the dedicated paper-only Cloud Run Job
entrypoint. It accepts exactly two explicit arguments:

```powershell
india-swing-operational-job `
  --spec-file C:\absolute\path\to\operational-job.json `
  --state-root C:\absolute\restored-state
```

The canonical job spec pins the proposal batch, portfolio artifact and
portfolio snapshot, target session, entry window, all three decision-policy
IDs, quote chunk size, maximum portfolio age, and expected operational spec
ID. A different policy version or reconstructed decision window fails before
Kite is contacted.

The state root uses a fixed layout rather than paths supplied inside the spec:

- `proposal_graph/` contains exact proposal manifests and parent archives;
- `portfolio/` contains sealed reconciled portfolio artifacts;
- `operational/` contains terminal run records;
- `decision_outbox/` contains local notification artifacts; and
- `paper/` contains the append-only paper ledger; and
- `notification_delivery/telegram/` contains create-once Telegram receipts.

The job requires a pre-restored, canonical, writable state root. On an
ephemeral Cloud Run filesystem, the state must be mounted or restored during
the same job invocation and subsequently backed up; a separate completed job's
local filesystem is not durable.

Runtime Kite credentials are read from `INDIA_SWING_KITE_API_KEY` and
`INDIA_SWING_KITE_ACCESS_TOKEN`. The private durable-output bucket is read from
`INDIA_SWING_OPERATIONAL_STATE_BUCKET`. Telegram delivery reads
`INDIA_SWING_TELEGRAM_BOT_TOKEN` and the numeric private-chat ID from
`INDIA_SWING_TELEGRAM_CHAT_ID`. Secrets are never written into a spec, content
identity, artifact, terminal record, receipt, or error response. The SDK
wrapper exposes only instruments, historical data, and quote reads; the
operational job uses only full quotes.

The portfolio artifact requires four distinct evidence bindings: broker funds,
broker positions, the engine risk ledger, and the engine P&L ledger. The current
verification status is explicitly `MANUAL_RECONCILED_PAPER_ONLY`; it records a
manual reconciliation but does not claim an automated broker reconciliation.
Missing, stale, future-dated, or differently identified portfolio evidence is
rejected before any quote request.

The job is idempotent for an operational spec ID. If a valid terminal record
already exists, its decision notification and, for BUY, its paper registration
are re-read and identity-checked before it is returned without requesting
quotes again. Missing or inconsistent terminal side effects fail closed. New
runs use the established order: decision outbox, paper registration where
applicable, then terminal operational record. The CLI emits IDs and status
only. It does not print credentials or upstream exception details.

After local terminal-last publication, the job publishes the notification,
optional BUY paper registration, and terminal record to GCS using immutable
create-or-verify writes. It publishes a small manifest last. The manifest pins
every object name, generation, byte count, and SHA-256; the JSON success output
contains the manifest object name, generation, and hash needed for restoration.
The operator or scheduler must retain that pin. Restoration never lists a
bucket or chooses a `latest` object.

An exact publication can be restored into another canonical state root with:

```powershell
india-swing-operational-restore `
  --state-root C:\absolute\restored-state `
  --expected-spec-id <sha256> `
  --manifest-object <exact-object-name> `
  --manifest-generation <positive-generation> `
  --manifest-sha256 <sha256>
```

Restoration downloads only the externally pinned manifest generation and the
generations named inside it. All hashes and cross-artifact lineage are checked
before local writes; the notification and optional paper registration are
created before the terminal record. Retries are idempotent and conflicting
local content fails closed.

The Docker image already installs the pinned Kite and GCS dependencies. Its
existing default command remains the daily evidence job; an operational Cloud
Run Job must explicitly override the command to
`python -m india_swing.operational_job`.

Every terminal result, including an operational failure, is sent to the
configured private Telegram chat after the GCS manifest is durable. Messages
use protected content, disable link previews, retain the full research/paper-
only warning, and include the immutable operational record ID. A local
create-once receipt suppresses ordinary retries against the same persisted
state root. Telegram Bot API `sendMessage` does not provide an idempotency key:
a process crash after Telegram accepts the message but before the receipt is
persisted—or a retry that does not restore the receipt—can cause a duplicate
alert. The system intentionally prefers a visible duplicate over silently
treating an unconfirmed alert as delivered. Never place the bot token in a job
spec or the repository.

No module in this package can place, modify, or cancel an order.
