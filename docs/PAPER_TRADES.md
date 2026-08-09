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

## Upstream: raw current-cross-section history window

Before any paper trade can be registered, a prospective decision needs its
current-universe subjects' raw price history assembled under an explicit
cutoff. `src/india_swing/forward_paper/history.py` is that bridge. It is
collection-only and several stages upstream of a `ShadowAlert`: it computes
no return, feature, label, rank, signal, or confidence, and grants no
training/feature/label/ranking/alert/paper-trade/notification/execution
authority.

A `ForwardPaperHistoryWindowSpec` pins one `dataset_id`, one `signal_session`
(the prospective decision's current-universe date), one canonical UTC
`decision_cutoff`, and one ordered tuple of exactly 60 unique,
strictly-increasing `expected_market_sessions` ending on `signal_session`.
Current-universe membership is defined solely by `signal_session` — the
spec never treats appearing in the earlier 59 sessions as proof of current
membership, and never treats current membership as proof of historical
availability.

`build_forward_paper_raw_history_window` consumes one caller-supplied
iterator of already-verified
`NseArchiveResearchPriceStreamSession` values (obtained from the accepted
`iter_nse_archive_research_price_stream_sessions` seam) exactly once. It may
skip sessions strictly before the first expected date, but from the first
expected date onward every session must agree exactly, one-for-one, with the
next pinned date and must have been observed no later than
`decision_cutoff` — a missing, duplicate, reordered, substituted, or
future-observed session fails closed immediately. Consumption stops the
instant the exact signal session is consumed; no later session is ever
pulled.

Every observation on the signal session — in its stored order, none dropped
— becomes exactly one outcome in the resulting `ForwardPaperRawHistoryWindow`:

- A `ForwardPaperHistoryCandidate` requires a resolved `research_identity_id`
  on the signal-session row and exactly one observation carrying that same
  identity in every one of the 60 expected sessions, retained in
  expected-session order by reference (prices, delivery, surveillance,
  identity, and transitions are never copied or recalculated). A symbol
  change for the same research identity is followed; a listing-key rebound
  to a *different* research identity is never joined, because matching is by
  research identity, never by listing key or symbol.
- A `ForwardPaperHistoryVeto` explains, with a fixed enum reason and exact
  lineage IDs, why a signal-session subject could not become a candidate:
  `SIGNAL_IDENTITY_UNRESOLVED` (the signal row itself is unresolved or a
  same-session ISIN collision), `REQUIRED_SESSION_MISSING` (the identity is
  absent from at least one of the 60 required sessions), or
  `REQUIRED_SESSION_DUPLICATED` (the identity appears more than once in one
  required session — checked before `REQUIRED_SESSION_MISSING`, so a subject
  with both problems is always reported as duplicated). No current subject
  ever silently disappears from the cross-section. Each veto also carries
  canonical, fixed-shape evidence IDs auditing its reason — never free text:
  the exact retained `price_stream_session_id` of every affected required
  session, plus (for `REQUIRED_SESSION_DUPLICATED` only) every duplicate
  `observation_id`, in deterministic expected-session/stored-observation
  order. The window independently re-derives the complete expected evidence
  by scanning all 60 retained sessions itself and requires exact tuple
  equality with what the veto carries, so a veto can never carry a real but
  unrelated session or observation ID, and can never pass by naming only a
  valid subset of a multi-session anomaly.

`ForwardPaperRawHistoryWindow` retains, by reference, the exact ordered
tuple of all 60 consumed `NseArchiveResearchPriceStreamSession` values — one
per pinned expected date, in that order, with the signal session always the
final entry. Every retained session is independently re-verified against
the spec's dataset and cutoff, and every candidate observation is
cross-checked against the exact observation actually present in its
claimed retained session — never merely another self-consistent object with
a matching date and identity. The window exposes exact aggregate counts
(`expected_session_count=60`, `consumed_session_count=60`,
`signal_subject_count`, `complete_candidate_count`, `veto_count`) and binds
all 60 session IDs, every candidate/veto ID, and dataset/spec lineage into
its own content identity, alongside the same fixed `collection_only=True` /
all-else-`False` posture reported by every type in this module.

Raw history assembled here is still `RAW_UNADJUSTED` and per-record
non-actionable. The next required stage, not implemented here, is
cutoff-aware corporate-action and tick-size adjustment before any
deterministic feature may be computed from this window.
