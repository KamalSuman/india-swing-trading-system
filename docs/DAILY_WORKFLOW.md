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

## Manual NSE historical archive import

The historical cash-equity importer accepts only three exact NSE Archive
evidence profiles observed in the official archive portal:

- full bhavcopy/delivery plus UDiFF bhavcopy;
- those two reports plus the NSE-only CM MII security master; or
- those three reports plus REG1 surveillance.

Every session records its exact profile and missing-evidence codes. Missing
REG1 is never interpreted as an empty surveillance set, and a missing security
master makes every row's same-session identity unresolved. The importer still
validates and reconciles only the declared `EQ` lane, preserves non-EQ
exclusions, and writes create-once market snapshots plus one immutable range
index. Source disagreements and unavailable identity evidence are retained as
blocking identity issues; they are never dropped, guessed, or repaired from a
later instrument list. Imported snapshots remain collection-only,
non-actionable, and training-ineligible.

A fourth, weaker `PRICE_DELIVERY_UNRECONCILED` lane exists for archives (for
example NSE's 2022 downloads) whose ZIP contains only the same-session full
bhavcopy/delivery report, with no UDiFF bhavcopy, NSE-only CM MII security
master, or REG1 surveillance file present. It preserves the full report's
observed OHLC, volume, turnover, trade count, and delivery values exactly as
published, but every row's missing evidence is recorded explicitly as
`UDIFF_BHAVCOPY`, `NSE_CM_SECURITY_MASTER`, and `REG1_SURVEILLANCE`, and every
row's identity status is `UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE`: no
UDiFF financial-instrument ID, no NSE-only security-master ISIN, and no REG1
surveillance indicator is ever present, and none of those may be filled in
from a later or current instrument list. Like every other profile it remains
collection-only, non-actionable, and training-ineligible, and it is
permanently ineligible for promotion until a separately reviewed, independent
same-session evidence process exists to reconcile it.

The `PRICE_DELIVERY_UNRECONCILED` lane accepts a second, equally weak
source-name shape for the pre-July-2020 official archive: the exact pair
`cmDDMMMYYYYbhav.csv.zip` (a nested ZIP containing exactly one
`cmDDMMMYYYYbhav.csv`) plus `MTO_DDMMYYYY.DAT`, never a mixture with any
modern name. The legacy Bhavcopy carries OHLC, traded quantity, traded value,
trade count, and an uncorroborated ISIN but no delivery figures; the MTO file
is NSE's separate legacy delivery-position report, and its own title,
session-date, header, and record-count/quantity control-total structure are
validated exactly. `average_price` and `turnover_lacs` are derived
deterministically from the legacy Bhavcopy's traded value and quantity under
an explicit local `Decimal` context, never the caller's ambient one, using the
same `ROUND_HALF_UP` two-decimal policy as every other profile. Delivery
quantity and percentage come from the MTO file, joined to the Bhavcopy by an
exact one-to-one `(symbol, series)` key match and exact traded-quantity
agreement over the declared `EQ` rows only; any missing, extra, or
quantity-disagreeing key rejects the whole session rather than dropping or
coercing a row. Every field this profile cannot corroborate -- including the
Bhavcopy's own ISIN -- is recorded exactly as unresolved, the same as the
single-file shape; a real, verifiable NSE holiday wrapper genuinely predating
July 2020 exists and must resolve to `PRICE_DELIVERY_UNRECONCILED` the same
way, never as a fabricated or coerced modern-format session. Like every other
profile it remains collection-only, non-actionable, training-ineligible, and
permanently ineligible for promotion until a separately reviewed, independent
same-session evidence process exists to reconcile it. Locally downloaded
Bhavcopy/MTO pairs sitting in a Downloads folder are not canonical evidence by
virtue of matching this naming convention; they become part of the record
only once staged, imported, and their range successfully replayed through
`verify-range`.

A read-only audit of 1,228 locally staged 2015-2019 legacy sessions (two
further sessions, 17-Jun-2019 and 18-Jun-2019, correctly and permanently fail
the exact EQ join) found four bounded official publisher variants, and the
parser now accepts exactly these four, never a generic relaxation:

- The legacy Bhavcopy also accepts the same 13 named columns with no
  trailing comma (so no terminal empty 14th field), selected by an exact
  header match; a file may not mix that shape's rows with the terminal-comma
  shape's rows.
- The MTO settlement-metadata line also accepts the same four labeled
  fields with the publisher's dropped-"T" typo (`rade Date` instead of
  `Trade Date`), and a two-field shape carrying only `Trade Date` and
  `Settlement Type` (`Settlement No`/`Settlement Date` genuinely absent, not
  fabricated) for sessions from 2019 onward. The parsed trade date must
  still equal the outer session in every shape.
- The MTO body also accepts exactly one trailing empty CSV record (a
  second terminal newline in the source file); an interior empty record or
  a second trailing one still fails closed.
- The published delivery percentage is bounded corroboration, not the
  validation authority: it is accepted when it differs from an explicit,
  caller-context-independent `ROUND_HALF_UP` recalculation by at most 0.01,
  and the published value -- never the recalculated one -- is what is
  stored. Every quantity control (`deliverable_quantity <= traded_quantity`,
  the exact EQ join, the MTO record-count/total-deliverable-quantity
  accounting) is unchanged and unaffected by this tolerance.

`NSE_HISTORICAL_ARCHIVE_IMPORTER_VERSION` moved to v5 for this widened
physical grammar; the evidence profile, schemas, and every other validation
ceiling are unchanged. This audit and the parser fix that followed it are not
a claim that the entire 2015-2019 corpus is now accepted -- that requires
Codex to rerun the real local audit and a full immutable range replay.

The immutable range index reports both `identity_issue_count` and
`identity_quarantined_session_count`. These counts are operational alarms, not
permission to repair history from a later session. A later NSE file may explain
an identifier transition, but only separately bound point-in-time evidence can
make a quarantined row eligible for research or trading.

An archive that fails session-date or cross-field validation is not silently
skipped or coerced. NSE can publish a holiday-dated wrapper whose only entry
contains the preceding session's full bhavcopy; that wrapper must be
quarantined rather than counted as another session. Preserve its official
outer ZIP outside the canonical snapshot set. Weekend special sessions and
Muhurat trading require separately bound calendar evidence before their price
rows can enter research; a weekday-only downloader is not a complete trading
calendar.

```powershell
python -m india_swing.market_data.nse_archive_cli import-range `
  --staging-root C:\project\india-swing-data\staging `
  --archive-root C:\project\india-swing-data\source-archives `
  --store-root C:\project\india-swing-data\canonical-market-data `
  --start 2026-01-01 `
  --end 2026-07-31 `
  --observed-at 2026-08-02T00:00:00+00:00 `
  --workers 4
```

`--observed-at` is mandatory and must describe the explicit historical-import
knowledge time; the importer never reads the ambient clock. An official outer
ZIP is preferred. A previously validated exact extracted entry set is accepted
only when the original ZIP is unavailable, and that weaker provenance mode is
recorded in the content-addressed payload.

Before an archive range is handed to an identity or research job, replay its
exact immutable index and every pinned session snapshot:

```powershell
python -m india_swing.market_data.nse_archive_cli verify-range `
  --store-root C:\project\india-swing-data\canonical-market-data `
  --index-snapshot-id <exact-index-snapshot-id>
```

The verifier never lists or selects a latest index. It rechecks index/session
lineage, byte-exact payload replay, collection-only safety flags, record lanes,
and identity-issue accounting. This complete replay is intentionally an
offline trust-boundary operation; recurring jobs should pin its accepted index
ID and process only newly appended immutable sessions.

`src/india_swing/evaluation/nse_archive_research_dataset.py` adds one further
offline layer above `verify-range`: `build_nse_archive_research_dataset`
binds an exact, caller-ordered tuple of already-pinned index snapshot IDs
into one immutable `NseArchiveResearchDataset` lineage/control manifest. It
is a compact manifest, not a materialized price panel -- it references exact
range and session snapshot IDs and aggregate counts, and it never lists,
discovers, or selects a latest index. As of this increment the pinned corpus
spans nine exact archive-range indexes covering 2015-01-01 through
2026-07-31: 2,849 accepted sessions and 4,792,827 EQ records. Three sessions
are explicit, permanently unresolved exclusions rather than silent gaps:
2018-10-12 (`SOURCE_ACCOUNTING_FAILED`, inconsistent authoritative MTO
accounting) and 2019-06-17 / 2019-06-18 (`SOURCE_CROSS_SOURCE_JOIN_FAILED`,
the exact cross-source EQ join failed). The first research split policy is
fixed and chronological: training sessions through 2022-12-31, validation
sessions from 2023-01-01 through 2024-12-31, and untouched test sessions
from 2025-01-01 onward; the final 20 sessions of every partition are
reserved as an unavailable forward-label tail (the maximum planned
forward-return horizon) and are never candidate label origins. Every dataset
safety flag -- `collection_only`, `actionable`, `training_eligible`,
`feature_eligible`, `label_eligible`, `alert_eligible`,
`execution_eligible`, `identity_resolution_complete`, and
`corporate_action_adjustment_complete` -- is hard-coded and not
caller-controllable; this manifest does not authorize model training or any
feature, label, alert, or execution generation until separate identity
resolution and corporate-action adjustment gates exist.

The manifest above can now be sealed locally. `research-dataset-build`
performs the complete exact replay -- it calls `build_nse_archive_research_dataset`
against `LocalMarketSnapshotStore`, which loads and re-verifies every pinned
range/session before sealing the compact manifest -- and then stores the
result exactly once through a create-once, exact-ID local store:

```powershell
python -m india_swing.market_data.nse_archive_cli research-dataset-build `
  --store-root C:\project\india-swing-data\canonical-market-data `
  --research-store-root C:\project\india-swing-data\research-datasets `
  --index-snapshot-id <exact-index-snapshot-id> `
  --index-snapshot-id <exact-index-snapshot-id> `
  --train-end 2022-12-31 `
  --validation-start 2023-01-01 `
  --validation-end 2024-12-31 `
  --test-start 2025-01-01 `
  --maximum-forward-label-horizon-sessions 20 `
  --source-accounting-failed-session <YYYY-MM-DD> `
  --source-cross-source-join-failed-session <YYYY-MM-DD>
```

`--index-snapshot-id` may repeat, and the supplied order is preserved exactly
as the chronological range order the manifest binds. This can be slow on the
real corpus -- it is local computation, not a cached "latest" shortcut, and
must not be replaced with one. The result is written under
`<research-store-root>/nse-archive-research-datasets/<dataset_id>.json`;
`put` is create-once and idempotent, and there is no list, glob, latest, or
overwrite path. To replay an already-sealed manifest without rebuilding it:

```powershell
python -m india_swing.market_data.nse_archive_cli research-dataset-show `
  --research-store-root C:\project\india-swing-data\research-datasets `
  --dataset-id <exact-dataset-id>
```

Both commands print only a deterministic JSON summary (counts, IDs, safety
flags, and per-partition role/session/candidate/tail counts) -- never a raw
record. Neither command changes the research-only boundary above: sealing
the manifest is still not model training, feature/label/alert generation, or
any action on the promoted engine.

`src/india_swing/evaluation/nse_archive_research_replay.py` adds the typed
replay boundary immediately above one already-sealed
`NseArchiveResearchDataset`:
`iter_verified_nse_archive_research_sessions(dataset, reader)` replays only
the dataset's explicitly pinned range and session snapshot IDs -- in their
exact stored order, one `load_verified_nse_historical_archive_range` call
per exact range index ID, never a list/latest/discovery lookup -- into an
iterator of immutable `NseArchiveResearchReplaySession` values, each holding
its ordered `NseArchiveResearchReplayRecord` projection with lossless
Decimal/int/None fields (unresolved identity, missing security-master
evidence, and incomplete-evidence sessions are retained, never filtered).
Only the one archive range currently being replayed is ever held in memory:
the iterator never accumulates multiple ranges, so consuming it partially or
stopping early never triggers a later range's load and never itself
constitutes a completed or publishable research artifact -- normal iterator
exhaustion after the dataset's final session is the only completion signal
this module provides. Every replayed session and record keeps the dataset's
exact safety posture (`collection_only=True`; `actionable`,
`training_eligible`, `feature_eligible`, `label_eligible`, `alert_eligible`,
and `execution_eligible` all `False`): this is a typed replay boundary for
later research feature materialization, not an authority upgrade, and it
still does not connect to the promoted engine.

### Legacy index-schema compatibility (`nse-historical-archive-eq-index/v1`)

`load_verified_nse_historical_archive_range` accepts three historical
index shapes: the current `.../v3` (claims an `evidence_profile_counts`
mapping and an `incomplete_evidence_session_count`), `.../v2` (claims a
top-level identity aggregate but no evidence-profile accounting), and the
oldest `.../v1` shape actually present in the imported local corpus
(`schema_version`, `range_start`, `range_end`, `collection_only`,
`actionable`, `training_eligible`, and `records`, each record carrying
only `session`, `snapshot_id`, `record_count`, and
`source_container_sha256` -- no claimed identity or evidence-profile
field at all). For a v1 index, every derived total
(`identity_issue_count`, `identity_quarantined_session_count`,
`evidence_profile_counts`, `incomplete_evidence_session_count`) is
computed by loading and independently re-verifying each pinned session
snapshot through the same `_verify_session` boundary v2/v3 already use --
never defaulted to zero and never trusted from an unverified claim,
because v1 made no such claim to trust. The per-session count fed into
that boundary, and accumulated into the v1 range total, is always the
value independently derived as `len(identity_issues)` for that session --
it is never reread from the session's own separate `identity_issue_count`
claim after verification, even when that claim matched. `.../v3`'s own
`evidence_profile_counts` mapping is normalized before comparison: a
known profile key that is missing from the claimed mapping is treated as
a claimed zero, so it agrees with the independently derived totals only
when the real derived count for that profile is actually zero (this is
exactly the shape of a real 2024 compact index, whose
`PRICE_DELIVERY_UNRECONCILED` key was omitted because no session in that
range ever needed it) -- an omitted key whose true derived count is
nonzero still fails closed, as does any unknown key or a non-integer,
negative, or boolean claimed value. `_verify_session` itself requires
every accepted schema's own claimed `identity_issue_count` to have exact
type `int` and be non-negative before any equality comparison runs, so a
`bool` claim (`True`/`False`) is rejected on type alone -- Python
considers `True == 1` and `False == 0`, so a naive numeric comparison
could otherwise silently accept a boolean claim whenever its numeric
value happened to match the real count.

This is entirely a **read-side verification rule** over the exact,
already-published, content-addressed snapshots already sitting in the
canonical store: it never rebuilds, migrates, rewrites, or repairs a
stored index or session snapshot, and it never widens what a
`collection_only` result is eligible for -- a range verified through a
legacy v1 index is exactly as `paper`/research-only, non-training,
non-actionable, and non-execution-eligible as one verified through the
current v3 shape.

## Deliberate boundary

This workflow closes the automated **EOD paper-outcome leg**. It does not yet
turn collection-only daily evidence into a live proposal batch. The signal
engine correctly rejects collection-only inputs; real proposal generation
still requires point-in-time promotion of stable identity, adjusted prices,
corporate actions, tick sizes, and universe evidence.

The exact promoted-evidence-to-proposal-graph bridge is now implemented:
`PromotedGraphPublisher`/`india-swing-promoted-graph-publish` assembles the
promoted identity/session/history/corporate-action graph into one durable,
exact-ID `PromotedGraphPublicationManifest`; `PromotedEngineRunner`/
`india-swing-promoted-engine` runs one signal-session paper research pass
from an already-published graph's two exact roots; and
`PromotedResearchOrchestrator`/`india-swing-promoted-research-run` derives
those roots directly from one exact `graph_manifest_id` and durably binds
both terminal manifests together, so a single restart-safe command chain now
runs end to end from published graph evidence to a bound paper research
result. The first research-to-paper-operation seam is now implemented too:
`prepare_and_publish`/`india-swing-promoted-operational-prepare` replays one
exact `PromotedResearchRunManifest` and materializes a durable, restart-safe
`PromotedOperationalPreparationManifest` whose candidate coverage exactly
matches the selected research intents (see
docs/PROMOTED_OPERATIONAL_PREPARATION.md). It deliberately stops at exact
quote keys and lineage -- it does not acquire a quote, create a
`SwingProposalBatch`, or grant actionability. The next increment is now
implemented too: `evaluate_promoted_operational_quote_gate` (see
docs/PROMOTED_OPERATIONAL_QUOTES.md) binds one exact operational
preparation to one already-acquired, exact `FullQuoteBatch` and produces
one PASS/VETO outcome per retained candidate using the same
proposal-independent quote-quality checks the legacy Swing quote gate uses,
plus promoted-intent-native limit/stop/target/tick checks. It still only
evaluates quotes -- it does not acquire a quote, rank a candidate, size a
position, produce a BUY decision, notify, register a paper trade, or
execute. The next increment is now implemented too:
`assemble_promoted_operational_allocation_batch` (see
docs/PROMOTED_OPERATIONAL_ALLOCATION.md) consumes one exact quote-gate
batch plus an exact, evidence-bound portfolio context and deterministically
allocates only PASS candidates under current cash, risk, exposure,
position, research-liquidity, and top-of-book constraints, reusing the
accepted `SwingPortfolioSnapshot`/`SwingPortfolioSizingPolicy` types
unchanged; operational quantity can only stay equal to or shrink from the
retained research quantity, and it still produces no BUY decision,
notification, persistence, or execution authority. The fourth and final
pure boundary before a runner exists is now implemented too:
`assemble_promoted_operational_decision_package` (see
docs/PROMOTED_OPERATIONAL_DECISIONS.md) consumes one exact allocation batch
and produces exactly one content-addressed `PAPER_BUY`/`NO_TRADE` decision
package with complete quantity-cap evidence, deterministically replayed
rationale and cancellation conditions, canonical `QUOTE:`/`ALLOCATION:` veto
coverage, and a human-readable advisory that always opens with `PAPER
RESEARCH ONLY — MANUAL REVIEW REQUIRED — DO NOT AUTO-EXECUTE`; more than one
allocated outcome is rejected as an integrity error, and the package still
cannot place an order, notify, or persist. The restart-neutral runner core
that composes all four pure boundaries is now implemented too:
`execute_promoted_operational_run` (see docs/PROMOTED_OPERATIONAL_RUNNER.md)
binds one exact quote-gate spec, allocation policy, expected quote/portfolio
source identity, and a Kite-safe quote-chunk ceiling, acquires only exact
read-only inputs through two injected ports plus an injected clock, runs the
existing quote-gate -> allocation -> decision-package chain exactly once,
and returns one content-addressed `COMPLETE` or sanitized `FAILED` result
retaining only the exact verified prefix of artifacts produced before
termination -- never exception text, credentials, URLs, or a source-provided
message. It still contains no persistence, notification, broker order, or
paper-ledger registration capability. The restart-safe local publication
layer around it is now implemented too: `publish_promoted_operational_result`/
`run_and_publish_promoted_operational_service` (see
docs/PROMOTED_OPERATIONAL_PERSISTENCE.md) derive one immutable advisory for
every `COMPLETE`/`FAILED` result, derive and create-once register a paper
trade only for a singular `COMPLETE` `PAPER_BUY`, publish every side effect
idempotently (advisory, then registration, then terminal record last), and
replay an existing sealed terminal without ever touching the clock, either
source property, or either acquisition method again. Local advisory
creation is not Telegram delivery and grants no notification or execution
authority. The independent trust anchor that sealed-terminal replay
requires is now implemented too: `promoted_terminal_binding.py`/
`promoted_terminal_binding_control_plane.py` (see
docs/PROMOTED_TERMINAL_BINDING_CONTROL_PLANE.md) seal one durable
`spec_id -> expected_terminal_id` binding through the existing
`StateObjectWriter` and read it back at restart through a new
generation-observe-then-pin port, with a proven-absence read path that can
never conflate corruption with a genuinely first run. The anchored session
now joins that control plane to local publication behind one schedulable
entry point too: `run_publish_and_anchor_promoted_operational_session`
routes solely on the remote anchor, delegates every local-terminal/binding
decision to the already-accepted publication service, and seals the
binding only after a fresh terminal-last publication -- never before, and
never on replay. The explicitly authorized outer paper-pilot boundary is now
implemented too: `india-swing-promoted-paper-pilot-job` composes the hydrated
job with one durable GCS delivery claim, post-publication Telegram delivery,
and a durable receipt (see `docs/PROMOTED_PAPER_PILOT.md`). A matching digest-
pinned, exact-secret-version Cloud Run deployment script exists, but
deliberately creates no scheduler. The remaining live-control milestone is safe
per-session rollover and scheduling; a static one-session launch must never be
replayed every day.
