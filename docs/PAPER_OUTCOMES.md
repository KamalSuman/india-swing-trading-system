# Conservative paper-outcome replay

The paper-outcome resolver is a pure decision engine. It reads immutable input
objects and returns one deterministic `PaperOutcomeReplay`; it does not write
the paper ledger, send a notification, access a broker, or select a latest
artifact.

## Operational outcome job

The pure resolver is now composed by an exact-ID, paper-only operational job.
`PaperOutcomeJobSpec` pins one registration, calendar materialization, tick-size
snapshot, ordered EOD artifact set, replay cutoff, conservative fill policy, and
the expected replay ID. `prepare_paper_outcome_job_spec` reads those immutable
objects once and seals the replay ID without writing the ledger.

`run_paper_outcome_job` then re-reads the same IDs, independently reconstructs
the instrument binding and observations, refuses a different replay ID, and
uses `reconcile_paper_outcome` for append-only ledger changes. A create-once
terminal record captures the complete event prefix, gross and estimated net
P&L, and the same evidence-based review shape for profitable and losing closed
outcomes. Missing market/sector/news evidence remains an explicit uncertainty;
the review never invents a catalyst or automatically retrains a model.

The record is realized-P&L evidence for the next explicit portfolio
reconciliation. It does not silently mutate a portfolio artifact because that
artifact also binds broker funds, positions, engine risk, and engine P&L
evidence.

`india-swing-paper-outcome-job` publishes the exact registration, event prefix,
and terminal record to the configured private GCS bucket using create-or-verify
writes, followed by a terminal manifest. It sends Telegram only after that
manifest is durable. The job has no broker order capability.

`india-swing-paper-outcome-restore` requires the externally retained manifest
object name, generation, and SHA-256. It never lists a bucket or chooses a
latest object. Every referenced generation and hash is verified before the
registration, events, and terminal record are restored locally in that order.

Runtime configuration:

```text
INDIA_SWING_PAPER_OUTCOME_STATE_BUCKET
INDIA_SWING_TELEGRAM_BOT_TOKEN
INDIA_SWING_TELEGRAM_CHAT_ID
```

Job invocation:

```powershell
india-swing-paper-outcome-job `
  --spec-file C:\absolute\path\to\paper-outcome-job.json `
  --evidence-root C:\absolute\restored-evidence-state `
  --state-root C:\absolute\restored-operational-state
```

Both roots must already exist, be canonical local directories, and contain the
exact archived objects named by the job. Neither path is read from the job spec.

## Daily paper portfolio

Daily preparation is split from execution. A canonical
`PaperPortfolioPreparationSpec` supplies the UTC cutoff, exact calendar
materialization ID, ordered EOD artifact IDs, conservative policy, per-registration
decision-time tick snapshot and listing identity, predecessor binding, and loss
limits. It is an evidence inventory, not permission to discover a latest file.

`prepare_paper_portfolio_batch` safely enumerates the complete local paper
registration set and derives its active `ALERTED`/`OPEN` set. Listing coverage
must match that set exactly: a missing active registration, an extra terminal
registration, or any malformed directory entry fails before evidence is read.
It then replays each registration against the pinned evidence to seal the
expected replay IDs and stores one canonical, create-once batch spec:

```powershell
india-swing-paper-portfolio-prepare `
  --spec-file C:\absolute\path\to\paper-portfolio-preparation.json `
  --evidence-root C:\absolute\restored-evidence-state `
  --state-root C:\absolute\restored-operational-state
```

The command returns the exact prepared batch file consumed by
`india-swing-paper-portfolio-job`. Once any portfolio state exists, a new
genesis is rejected. A follow-on batch must bind the unique current leaf's
batch and state IDs, preventing accidental P&L resets or forks from an older
state. No evidence ID is inferred from filesystem time, name ordering, a GCS
listing, or a `latest` alias.

The daily-pipeline bridge removes the remaining hand-built inventory step. It
requires one exact `DailyPipelineRun` ID and one exact `DailyDerivedEvidence`
ID, validates the complete predecessor run chain, calendar materialization,
historical artifacts, current derived tick bundle, and the unique portfolio
leaf, then writes both the preparation spec and executable batch spec:

```powershell
india-swing-paper-portfolio-from-pipeline `
  --run-id <64-hex-daily-run-id> `
  --derived-evidence-id <64-hex-derived-evidence-id> `
  --evidence-root C:\absolute\restored-evidence-state `
  --state-root C:\absolute\restored-operational-state
```

For each new registration, the bridge derives its listing only from tick
snapshots reconstructed from that exact sealed run chain and chooses the final
snapshot whose `knowledge_time` is no later than the original decision time.
If no such snapshot exists, preparation stops; it never substitutes today's
snapshot. Existing positions retain the tick snapshot, series, and validated
ISIN already pinned by their preceding outcome job, and any identity drift is
rejected. The bridge currently accepts only the scanner's `EQ` series policy.

`PaperPortfolioBatchSpec` groups an explicitly ordered set of exact outcome-job
specs under one UTC cutoff. A follow-on batch must bind the exact previous batch
and state IDs. Every previously `WAITING`, `OPEN`, or `BLOCKED` registration must
remain in the next batch; omitting an active position fails before new evidence
is read. The batch never discovers registrations, lists storage, or selects a
latest object.

`run_paper_portfolio_batch` runs those exact jobs, retains earlier terminal
positions, and creates one immutable `PaperPortfolioState`. It accounts for a
position's realized P&L only on the first batch in which it becomes `CLOSED`, so
retries and later batches do not count the same outcome twice. The state
recomputes and verifies daily and cumulative estimated realized P&L, peak and
drawdown, estimated costs, open planned risk/notional, win rate, expectancy,
and configured daily/cumulative loss halts. These remain paper-fill estimates,
not broker performance or a return guarantee.

The operational entrypoint processes the batch, publishes every exact outcome
record, publishes the aggregate portfolio state, and only then sends its
Telegram summary:

```powershell
india-swing-paper-portfolio-job `
  --spec-file C:\absolute\path\to\paper-portfolio-batch.json `
  --evidence-root C:\absolute\restored-evidence-state `
  --state-root C:\absolute\restored-operational-state
```

Aggregate state is uploaded create-once before its terminal manifest. Restore
requires the externally retained batch ID, manifest path, generation, and
SHA-256; it verifies strict canonical JSON and every pinned object before making
a local create-once state visible:

```powershell
india-swing-paper-portfolio-restore `
  --state-root C:\absolute\restored-operational-state `
  --expected-batch-id <64-hex-batch-id> `
  --manifest-object <exact-object-name> `
  --manifest-generation <positive-generation> `
  --manifest-sha256 <64-hex-sha256>
```

Both commands use `INDIA_SWING_PAPER_OUTCOME_STATE_BUCKET`; the daily job also
uses the Telegram variables listed above. Neither entrypoint imports a broker
order client or has execution authority.

## Evidence boundaries

Before replay, one `PaperTradeRegistration` is bound to an exact NSE listing
through a `CollectionTickSizeSnapshot`. The resulting binding carries the
symbol, series, validated ISIN, session-scoped financial instrument ID, tick
size, tick observation ID, tick snapshot ID, and the tick observation's
knowledge time. Tick evidence known after the original decision is rejected.

Each daily observation is then derived from:

- one integrity-verified `NseEodSessionArtifact`;
- one integrity-verified `CalendarSnapshot`;
- the exact paper-instrument binding.

An observation records the source artifact and bar identities, session close,
knowledge time, listing identity, and raw OHLCV. A verified artifact with no
matching traded row becomes an explicit missing-bar observation instead of
silently disappearing.

Replay accepts only session-ordered observations from one calendar and rejects
calendar-session gaps. Observations whose knowledge time is after `as_of` are
not used. The calendar cutoff and tick knowledge time must also satisfy their
respective point-in-time boundaries.

## Conservative fill policy

- Entry is a simulated limit at `entry_high` with adverse slippage and a
  participation cap.
- A session opening below `entry_low` does not fill; the approved range is not
  widened after the alert.
- If stop and target are both possible in one daily bar, stop wins.
- A same-entry-session target is deferred because OHLC cannot prove it happened
  after entry.
- A gap through stop uses the adverse gap-open price.
- A matured holding horizon exits at the final close with adverse slippage.
- A missing horizon bar blocks the replay instead of inventing an exit.

The policy itself is content-addressed. Registration, binding, observation,
policy, and replay verification reconstruct their objects defensively, so an
invalid post-construction mutation cannot be hidden by recomputing an inner ID.

## Reconciling a replay with the paper ledger

`reconcile_paper_outcome(ledger=..., replay=...)` in `paper_outcomes/reconciliation.py`
is the only bridge between a pure replay and the append-only
`LocalPaperTradeLedger`. It never touches a broker, the network, or the
filesystem directly; every write goes through the already-reviewed
`LocalPaperTradeLedger.append()`.

It first verifies the replay's own content identity, then loads the exact
registration named by `replay.registration_id` from the ledger. `WAITING` and
`BLOCKED` replays always return a `NO_CHANGE` result and write nothing — even
when a `BLOCKED` replay carries a computed entry fill (for example, a missing
horizon bar), that fill is not persisted, since the status is not confirmed
`OPEN`.

For `OPEN`, `CLOSED`, and `EXPIRED` replays, reconciliation derives the exact
event prefix the replay implies:

- `EXPIRED` -> one `EXPIRED` event at `replay.as_of` with reason
  `ENTRY_WINDOW_EXPIRED_UNFILLED`, carrying replay lineage but no fill
  evidence or `market_session`.
- `OPEN` -> one `ENTRY_RECORDED` event from `replay.entry`.
- `CLOSED` -> `ENTRY_RECORDED` from `replay.entry` followed by
  `EXIT_RECORDED` from `replay.exit`, whose reason code
  (`STOP_EXIT`/`TARGET_EXIT`/`TIME_EXIT`) is derived from the exact replay
  exit reason.

Every automated event carries the replay's `replay_id`, `policy_id`,
`binding_id`, and `calendar_snapshot_id` as its ledger lineage fields.

Reconciliation is a **prefix operation**: any events already in the ledger are
compared field-for-field against this replay's expected prefix. An exact
match is idempotent — rerunning the identical replay appends nothing and
returns the existing chain unchanged, byte-for-byte. A mismatching prefix
(wrong price, evidence, session, lineage, an unexpected event type, or a
pre-existing manual event) raises `PaperOutcomeReconciliationError` and
appends nothing. Only the missing suffix is ever appended, which is how a
crash between writing the entry and the exit recovers cleanly: rerunning the
same `CLOSED` replay appends just the missing exit.

The stored `ENTRY_RECORDED` event's `replay_id` is exempt from this prefix
match: it keeps identifying whichever earlier replay first caused that
create-once event, and reconciliation never rewrites or relabels it. Every
other entry field — `occurred_at`, `observed_price`, `evidence_id`,
`reason_code`, `market_session`, `outcome_policy_id`, `instrument_binding_id`,
and `calendar_snapshot_id` — must still match exactly. This lets an entry
persisted from an earlier `OPEN` replay evolve into a later `CLOSED` replay
for the same registration by appending only the exit: a
`PaperOutcomeReplay`'s content identity necessarily changes whenever `as_of`,
`status`, source observations, or the exit fill changes, so this is normal
forward lifecycle evolution, not tampering. `replay_id` itself remains
event-specific — each ledger event still carries the exact replay that
produced it, and an existing `EXIT_RECORDED` or `EXPIRED` terminal event still
requires an exact match on every field, including `replay_id`: a later replay
can never replace or reinterpret an existing terminal outcome.

## Current limitation

All current NSE EOD artifacts are `RAW_UNADJUSTED`, collection-only records and
do not carry verified sell-circuit state. Consequently every replay is always
`PAPER_ONLY`, `provisional=true`, and `actionable=false`, with explicit blockers
for raw prices, unapplied corporate actions, collection-only acquisition, and
unavailable sell-circuit status. These replays are engineering observations,
not reportable performance or broker results. Reconciling them into the paper
ledger only records these provisional observations; it never authorizes or
simulates a broker instruction.
