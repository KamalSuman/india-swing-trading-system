# Restart-safe daily paper workflow

`india-swing-daily-workflow-job` composes the sealed daily-pipeline evidence
bridge, conservative paper-outcome replay, aggregate portfolio accounting,
immutable GCS publication, and Telegram delivery into one restart-safe EOD
job. It remains strictly `PAPER_ONLY` and has no broker-order capability.

## Exact invocation

```powershell
india-swing-daily-workflow-job `
  --run-id <exact-daily-run-sha256> `
  --derived-evidence-id <exact-derived-evidence-sha256> `
  --evidence-root C:\absolute\restored-evidence `
  --state-root C:\absolute\restored-state
```

Optional arguments configure the paper risk ledger without granting trading
authority:

```text
--daily-loss-limit 1000
--cumulative-loss-limit 2000
--maximum-attempts 3
```

Runtime-only configuration remains outside the evidence:

```text
INDIA_SWING_PAPER_OUTCOME_STATE_BUCKET
INDIA_SWING_TELEGRAM_BOT_TOKEN
INDIA_SWING_TELEGRAM_CHAT_ID
```

The bucket name is bound into the workflow specification so the same workflow
identity cannot silently publish to another durable destination. Tokens and
credentials are never stored in specifications, events, terminals, IDs, or
error output.

## Durable attempt state

Each exact workflow has a create-once specification, append-only attempt
events, and at most one terminal record. Every attempt records:

- `STARTED` before domain work;
- `COMPLETED` after the terminal record for a completed portfolio;
- `REJECTED` for evidence/invariant rejection or a valid no-active-position
  result; or
- `FAILED` with a fixed sanitized reason for operational failure.

The configured retry budget counts durable `STARTED` events. Once consumed,
the runner fails without contacting GCS, Telegram, or an evidence source again.
A crash after the terminal record but before its completion event is repaired
on restart without rerunning domain work.

The domain stages themselves are idempotent:

1. Exact daily-run and derived-evidence validation.
2. Create-once preparation and batch specifications.
3. Append-only paper-event reconciliation.
4. Create-once aggregate portfolio state.
5. GCS create-or-verify objects and terminal manifests.
6. A create-once local Telegram receipt.

A failure between stages can therefore be retried without creating another
logical portfolio result. Telegram cannot provide true remote idempotency, so
a process crash after Telegram accepts a message but before the local receipt
is durable can still produce a duplicate; the workflow never treats an
unconfirmed message as delivered.

When no `ALERTED` or `OPEN` paper registrations exist, the workflow does not
invent an empty portfolio genesis. It stores a terminal
`NO_ACTIVE_POSITIONS` result and sends one idempotent paper-only heartbeat.

## Cloud Run boundary

The entry point is directly suitable for a Cloud Run Job command override:

```text
python -m india_swing.daily_workflow_job
```

The job still requires the exact restored local evidence and state roots. It
does not list GCS objects, select a latest artifact, download unspecified NSE
data, or place an order. The scheduler must supply the exact daily run and
derived evidence IDs produced by the preceding collection job.

## Deliberate boundary

This workflow closes the automated **EOD paper-outcome leg**. It does not yet
turn collection-only daily evidence into a live proposal batch. The signal
engine correctly rejects collection-only inputs; real proposal generation
still requires point-in-time promotion of stable identity, adjusted prices,
corporate actions, tick sizes, and universe evidence. The next engine
milestone is the exact promoted-evidence-to-proposal-graph bridge, followed by
the already-built paper-only operational quote/decision job at the next
session's entry window.
