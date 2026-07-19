# Daily collection pipeline

The daily pipeline turns one explicit NSE CM session into a sealed collection
report. It orchestrates the existing strict importers and materializers; it does
not promote manual files, assign stable identities, generate a trade, or execute
an order.

## Bias boundary

The runner never scans a folder for the newest file and never selects an
implicit latest artifact. Every successor names one exact `previous_run_id`.
The supplied calendar must prove that predecessor is the immediately preceding
trading session. The predecessor carries forward the exact ordered security-
master and daily-bundle artifact chains, after which the current artifacts are
appended. A duplicate, skipped session, future cutoff, mismatched master date,
or missing current-session Bhavcopy fails closed.

A first run may omit `--previous-run-id`. It is explicitly marked
`NO_PREVIOUS_DAILY_RUN` and is only a bootstrap collection diagnostic. If a
calendar was first observed after the market session, the run cutoff must remain
later than that calendar knowledge time; the runner will not backdate it.

## One-command derivation

The `run` command:

1. imports the exact current security master and Multiple File Download ZIP;
2. derives positive traded-date evidence for the current bundle;
3. materializes the current raw, unadjusted EOD session;
4. reconciles the current master against the complete predecessor bundle chain;
5. rebuilds the ordered cross-vintage identity registry;
6. publishes the complete identity-adjudication queue;
7. writes one create-once, content-addressed completeness report.

The report is stored under `var/daily_pipeline/runs` by default. Override this
with `INDIA_SWING_DAILY_PIPELINE_ROOT`. The report binds every upstream artifact
ID, output ID, count, blocker, cutoff, calendar, and predecessor run. Reads
verify the report's own content identity and reject links, unexpected files,
duplicate JSON keys, mutation, or a path/content mismatch. Upstream artifact
stores retain their own raw-byte replay contracts.

Browser duplicate names such as `Reports-Daily-Multiple (1).zip` are accepted
only through the pinned `Reports-Daily-Multiple (positive integer).zip` pattern.
The source is read with stable-file checks and copied to a private temporary
official basename before the strict bundle importer runs. The original file is
never modified.

## Commands

Bootstrap one already-collected session:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.daily_pipeline.cli run `
  --session 2026-07-15 `
  --cutoff 2026-07-16T15:45:00+05:30 `
  --calendar-id <calendar-materialization-id> `
  --security-master-file C:\path\to\NSE_CM_security_15072026.csv.gz `
  --daily-bundle-file C:\path\to\Reports-Daily-Multiple.zip
```

Run the immediately following session:

```powershell
python -m india_swing.daily_pipeline.cli run `
  --session 2026-07-16 `
  --cutoff 2026-07-16T20:07:55+05:30 `
  --calendar-id <calendar-materialization-id> `
  --security-master-file C:\path\to\NSE_CM_security_16072026.csv.gz `
  --daily-bundle-file "C:\path\to\Reports-Daily-Multiple (1).zip" `
  --previous-run-id <exact-15-July-run-id>
```

Inspect persisted reports without rerunning the expensive upstream derivation:

```powershell
python -m india_swing.daily_pipeline.cli show --run-id <run-id>
python -m india_swing.daily_pipeline.cli list
```

CLI failures expose only a sanitized exception type. No partial daily-run report
is published when an upstream stage fails.

## Internal verified-landing-inputs entry point

`run_daily_pipeline_from_landing_inputs` (in `daily_pipeline/runner.py`) is a
second, internal entry point alongside `run_daily_pipeline`. It consumes an
exact, already-verified `VerifiedLandingInputs` value instead of caller-
supplied filesystem paths.

Before any artifact-store mutation it independently re-verifies the supplied
`VerifiedLandingInputs` (rejecting a mutated or wrong-type value), requires its
`market_session` and `run_cutoff` to equal the requested run's session and
cutoff, and builds the run's `LandingInputLineage` via
`build_landing_input_lineage`. Only then does it materialize the two already-
verified byte payloads into a private temporary directory, using the canonical
NSE basenames taken from that freshly built lineage and exclusive file
creation, and hand them to the same import/derive pipeline stages
`run_daily_pipeline` uses. It never lists a bucket, selects a "latest" object,
retries, or falls back to a second source.

Runs produced this way persist the exact `LandingInputLineage` on
`DailyPipelineRun` and omit `VERIFIED_LANDING_LINEAGE_UNAVAILABLE` from
`completeness_issues`. `run_daily_pipeline`'s manual-file behavior is
unchanged: `landing_input_lineage` remains `None` and the blocker remains
present.

This is an internal integration boundary only. There is no CLI command and no
production GCS wiring yet -- constructing a real `LandingObjectReader`,
resolving a manifest, and calling `acquire_verified_landing_inputs` from a
command remain separate future work.

## Current limitations

- The runner consumes manually downloaded, collection-only evidence.
- The calendar is locally observed and not point-in-time verified.
- Independent upstream stores deliberately replay raw sources; the current real
  two-vintage run takes several minutes on the development machine.
- Missing report vintages remain blockers. For example, the present archive
  lacks `REG1_IND150726.csv`, so 16 July effective surveillance state is not
  substituted from another date.
- Scheduling, authorized downloading, cloud object immutability, monitoring,
  and notifications are not implemented by this local command.
