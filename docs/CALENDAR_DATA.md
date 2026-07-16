# Event-sourced NSE cash-market calendar

Status: the source importer and deterministic event-graph materializer are
implemented for collection and diagnostics. Manual inputs remain
`COLLECTION_ONLY`, `actionable=false`, and cannot authorize a trade.

## Why the calendar is event-sourced

Weekday arithmetic and a current holiday table are not enough. NSE can publish
an annual holiday circular and later amend a date, add an unscheduled closure,
or replace a holiday/weekend with a special live session. The resolver therefore
requires an explicit chain such as:

```text
regular schedule -> holiday -> special live session
```

Every arrow names the exact content-derived predecessor event ID. Competing
branches, missing predecessors, cycles, overlapping base schedules, uncovered
dates, and implicit "latest file wins" behaviour fail closed. Mock or
contingency activity is retained as non-executable evidence and can never create
a live session.

## Manual source envelope

One import consists of:

1. the exact official PDF bytes; and
2. a strict UTF-8 JSON declaration transcribing the relevant events and binding
   the PDF filename, byte count, and SHA-256.

The declaration accepts only four event variants:

- `BASE_WEEKLY_SCHEDULE`;
- `DATE_CLOSED`;
- `DATE_SESSION_REPLACED`;
- `NON_EXECUTABLE_ACTIVITY`.

Dates, windows, phases, source page/section locators, document ID, reason, and
supersession IDs are typed and content-addressed. The importer archives the raw
PDF, raw declaration, deterministic normalized form, and canonical manifest,
and fully reparses them on every read. A claimed issue date later than local
observation is rejected.

The declaration is still an operator transcription. Binding it to a PDF proves
which bytes were consulted, not that every PDF sentence was transcribed
correctly. Its knowledge time is therefore the locally observed successful
validation time, never the circular's printed date.

Import one pair with:

```powershell
python -m india_swing.calendar_data.cli source-import `
  --source-pdf C:\path\to\CMTR-circular.pdf `
  --declaration C:\path\to\CMTR-circular.events.json
```

The response returns the source artifact ID and every content-derived event ID.
Later declarations use those IDs in `supersedes_event_ids`.

## Materialization semantics

Materialization takes explicit source artifact IDs, a UTC-normalized cutoff, and
contiguous coverage bounds. It requires one applicable base schedule for every
date, resolves the unique explicit state chain, and emits a content-addressed
NSE CM `CalendarSnapshot` plus complete per-day event/source lineage.

Paired UDiFF/full-Bhavcopy positive-date evidence can be supplied as a
cross-check. A positively traded date that resolves to a closed state is a hard
conflict. Absence of a report never creates a holiday.

The resulting session windows describe the exchange schedule only.
`CalendarDay.data_ready_at` remains null because a holiday/timing circular does
not prove when Kite, UDiFF, or another provider finalized its data. A separately
sourced finality policy is still required for decisions.

After importing every required source, materialize a bounded calendar:

```powershell
python -m india_swing.calendar_data.cli materialize `
  --source-id <base-source-id> `
  --source-id <holiday-or-amendment-source-id> `
  --coverage-start 2026-07-15 `
  --coverage-end 2026-07-31 `
  --cutoff 2026-07-16T00:00:00+05:30
```

Optional repeated `--observed-daily-bundle-id` arguments add positive traded-date
cross-checks. The sealed materialization store reopens every source and daily
bundle and reproduces the exact calendar bytes on each read.

## Promotion boundary

Manual acquisition and human declarations cannot become
`POINT_IN_TIME_VERIFIED`. Promotion additionally requires authenticated or
licensed acquisition evidence, a pinned source parser, complete annual and
amendment coverage, special-session windows, positive-date reconciliation, and
daily pre-use revalidation. Until then, reconciliation may use the calendar to
diagnose next-session effects, but its calendar-readiness blocker remains.
