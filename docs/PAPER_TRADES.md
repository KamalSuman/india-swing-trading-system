# Append-only paper-trade outcomes

The paper-trade ledger records what happened after a research-only candidate
alert. It is an audit and measurement component, not an execution component.

Registration accepts only an exact, integrity-verified `ShadowAlert` whose kind
is `CANDIDATE` and whose decision is not execution eligible. It creates one
immutable record binding the alert, pipeline and decision identities together
with the simulated symbol, quantity, entry window, entry range, stop, target,
holding horizon, and estimated round-trip cost.

The lifecycle is deliberately small:

```text
ALERTED -> ENTRY_RECORDED -> EXIT_RECORDED
    |             |
    +-> EXPIRED   +-> INVALIDATED
    +-> INVALIDATED
```

Entry and exit observations require a positive `Decimal` price and a full
SHA-256 evidence ID. `occurred_at` is the evidence's knowledge/record time, not
an invented intraday fill timestamp; entry and exit fills separately carry a
`market_session` date, and it is `market_session` — not `occurred_at` — that
governs the entry window, exit ordering, and same-session rules. Entry is
accepted only when its `market_session` falls inside the registered IST entry
window and its price is inside the alert's approved range. Exit requires a
prior entry and a `market_session` no earlier than the entry's session.

A later-session exit must also carry independent evidence (a different
`evidence_id` from entry). A same-session exit is rejected unless it is an
automated `STOP_EXIT` produced by the same replay as the entry (matching
`replay_id`) with an exit price no greater than the registered stop — the one
case where entry and exit legitimately share the same observation and
evidence, because the stop was breached on the same bar that filled the entry.
Same-session `TARGET_EXIT` or `TIME_EXIT` is always rejected, automated or
not, since daily OHLC cannot prove a same-bar target happened after entry.
Expiry requires the entry window to have elapsed. Closed, expired, and
invalidated records are terminal.

An automated event produced by outcome reconciliation additionally carries
`replay_id`, `outcome_policy_id`, `instrument_binding_id`, and
`calendar_snapshot_id` — the four lineage IDs are either all present or all
absent. A fill event (`ENTRY_RECORDED`/`EXIT_RECORDED`) always requires
`market_session`; a non-fill event (`EXPIRED`/`INVALIDATED`) never carries one,
even when automated expiry carries replay lineage without fill evidence.
`INVALIDATED` can never carry automated lineage, since reconciliation never
produces it. See `docs/PAPER_OUTCOMES.md` for the reconciliation contract that
writes these automated events.

The registration filename is its own content identity. Each event is likewise
content-addressed, predecessor-linked, and written create-once.
On every read the ledger validates filenames, exact JSON schemas, event IDs,
sequence continuity, predecessor links, monotonic timestamps, and the legal
state transition history. Altered or extra files fail closed.

`PaperTradeSummary` reports `ALERTED`, `OPEN`, `CLOSED`, `EXPIRED`, or
`INVALIDATED`. Gross and estimated-net P&L exist only after an exit; estimated
net P&L subtracts the alert's planned round-trip cost and is explicitly not a
broker statement or realized account result.

There is no broker client, credential access, notification sender, GCP writer,
order method, or authority flag in this package.
