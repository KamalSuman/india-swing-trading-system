# Conservative paper-outcome replay

The paper-outcome resolver is a pure decision engine. It reads immutable input
objects and returns one deterministic `PaperOutcomeReplay`; it does not write
the paper ledger, send a notification, access a broker, or select a latest
artifact.

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
