# Calendar evidence and collection reconciliation

Status: the pipeline can now prove positive traded dates and reconcile every
retained security-master row against the archived daily reports. Both outputs
are diagnostic only: `COLLECTION_ONLY`, `actionable=false`, and intentionally
incompatible with trade generation.

## Positive market-date evidence

The daily bundle contains paired, row-confirmed UDiFF and full Bhavcopies. They
can prove that trading data exists for a date, but they cannot prove that an
absent date was a holiday, identify a regular versus special session, establish
session windows, or resolve the next trading session.

Build the evidence with an explicit cutoff:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.reconciliation.cli observed-dates `
  --daily-bundle-id <bundle-artifact-id> `
  --cutoff 2026-07-15T14:37:15.502701+00:00
```

The report date is event time. The bundle's successful `validated_at` is the
conservative knowledge time. A final UDiFF/full pair is not accepted before
20:00 IST on its claimed trade date. This is a deliberately late collection
guard against future/intraday final-report leakage; it is **not** a claim about
the exchange close or NSE's actual publication SLA. A verified calendar and
source-specific data-ready policy must eventually replace it. The output
deliberately exposes no weekday, holiday, session-kind, session-window, or
`next_session` operation.

## Collection-only listing reconciliation

The reconciler starts from every `RETAINED_UNVERIFIED_EQUITY` security-master
row. It never starts from traded Bhavcopy membership, so a security with no trade
on the target date is not silently deleted from the population.

```powershell
python -m india_swing.reconciliation.cli reconcile `
  --security-master-id <security-master-artifact-id> `
  --daily-bundle-id <bundle-artifact-id> `
  --market-session 2026-07-15 `
  --cutoff 2026-07-15T14:37:15.502701+00:00
```

The pure reconciler:

- requires the master filename claim to match the target session;
- reopens each artifact by ID through its sealed local store, rejects duplicate
  availability partitions, reparses the archived raw ZIP/gzip, and verifies the
  complete manifest and normalized output; a substituted parsed tree,
  recomputed availability timestamp, or forged counter is rejected;
- joins only the same-vintage `(symbol, series)` key, checks reverse instrument
  ID/unique-ISIN mappings, and rejects contradictory instrument ID, ISIN, or
  board-lot fields in UDiFF; descriptive-name differences are preserved as the
  row-level `UDIFF_MASTER_INSTRUMENT_NAME_MISMATCH` reason because NSE's master
  and UDiFF name fields can legitimately use different representations;
- embeds the exact master and daily-bundle manifests as paired lineage, and
  requires the entry count to equal the master's retained-row count;
- assigns exactly one diagnostic disposition to every retained master row and
  enforces unique listing keys/instrument IDs plus the pinned EQ/SM scope map;
- treats UDiFF/full rows as positive trade and delivery evidence only;
- records report rows absent from the master as orphans, never new members;
- requires orphan keys to be disjoint from retained membership;
- binds every evidence row to its parsed listing key, preventing a same-family
  row from being reassigned to another stock in a rebuilt snapshot;
- preserves REG1 dimensions independently rather than collapsing GSM, ASM, ESM,
  IRP, status, and other flags into a permissive state;
- rejects conflicting dated report vintages, SME/complete-band contradictions,
  and target band changes whose `To` state disagrees with the resolved complete
  band;
- binds raw hashes, normalized hashes, artifact IDs, manifest IDs, validation
  times, report rows, and the requested cutoff into one deterministic snapshot;
- has no method that creates an actionable universe.

REG1 and `sec_list` dated D apply on the next verified session. Without a real
calendar they remain candidate observations and cannot become effective state.
SME/band-change filename dates remain explicit unverified claims. The mutable
series-change file is processed as an event-sourced union: later absence cannot
erase an earlier positive transition, repeated events retain their earliest
known lineage, and contradictory transitions for the same symbol/date fail
closed. It remains corroborative evidence only and cannot establish stable
instrument identity.

## Current real-file diagnostic

For the supplied 15 July 2026 inputs, the earliest common cutoff produces:

- 21,133 retained master rows, all accounted for;
- 3,510 broad `EQ` scope rows, including small-cap companies;
- 772 `SM` scope rows for later SME watch-only policy;
- 16,851 other series explicitly outside the pinned swing scope;
- 2,834 retained rows with same-session UDiFF evidence;
- 2,686 report keys recorded as orphans rather than imported into membership;
- 4,282 supported `EQ`/`SM` rows unresolved because no verified calendar exists;
- zero actionable rows.

The absence of a trade row does not remove a master row. For example,
`RSDFIN-EQ` remains present in the diagnostic despite having no 15 July UDiFF or
full-delivery row.

## Promotion boundary

The next required input is an event-sourced NSE CM calendar built from the
regular schedule, annual holiday circular, later amendments/closures, and
special-session circulars. After that, the pipeline still needs an audited
cross-vintage identity registry, corporate actions, historical daily vintages,
and a liquidity policy before it can construct a point-in-time universe.
