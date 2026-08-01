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

The historical cash-equity importer consumes only an exact four-file NSE
Archive session: full bhavcopy/delivery, UDiFF bhavcopy, REG1 surveillance,
and the NSE-only CM MII security master. It validates and reconciles only the
declared `EQ` lane, preserves non-EQ exclusions, joins same-session identity
evidence, and writes create-once market snapshots plus one immutable range
index. Source disagreements are retained as blocking identity issues; they are
never dropped or guessed. Imported snapshots remain collection-only,
non-actionable, and training-ineligible.

The immutable range index reports both `identity_issue_count` and
`identity_quarantined_session_count`. These counts are operational alarms, not
permission to repair history from a later session. A later NSE file may explain
an identifier transition, but only separately bound point-in-time evidence can
make a quarantined row eligible for research or trading.

An archive that fails ordinary-session cross-field validation is not silently
skipped or coerced. Preserve its official outer ZIP outside the canonical
snapshot set and quarantine the staged session for explicit review. Special
sessions such as Muhurat trading require separately bound calendar evidence and
a dedicated policy before their price rows can enter research.

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
never on replay. The remaining milestone is explicitly authorized Telegram
delivery plus real storage-client construction and deployment wiring
around this now-complete restart-safe chain -- that wiring does not exist
yet.
