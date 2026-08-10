# Promoted paper-pilot first-session package

`src/india_swing/promoted_paper_pilot_session_package.py` (pure request/
codec model plus coordinator) and
`src/india_swing/promoted_paper_pilot_session_package_cli.py` (sanitized
offline CLI, `india-swing-promoted-paper-pilot-session-package`) compose
one deterministic, entirely offline first-session packager.

Starting from one exact, already-published promoted research run plus
one explicitly reconciled empty paper-portfolio genesis, this packager
publishes exactly the three artifacts the already-accepted cloud-control/
input-publication/deployment path requires: the operational preparation,
the portfolio artifact, and the promoted operational assembly spec.

**This is not new trading, ranking, risk, eligibility, cloud, broker, or
notification logic.** Every financial/identity computation is delegated
to three already-accepted boundaries, composed here in strict sequence
and never duplicated:

1. `prepare_and_publish` (`promoted_operational_preparation`) resolves
   the exact research run and durably publishes its paper-only
   operational preparation.
2. `seal_promoted_paper_portfolio_genesis`
   (`promoted_paper_portfolio_genesis`) archives the four exact evidence
   payloads and durably seals the initial, empty, manually reconciled
   paper portfolio artifact.
3. `prepare_promoted_operational_launch` +
   `publish_promoted_operational_launch_assembly_spec_file`
   (`promoted_operational_launch`) construct the existing
   `PromotedOperationalLaunchRequest` from this package's explicit
   controls plus the two freshly resolved IDs, dry-assemble it, and
   publish the resulting assembly spec create-once.

Because stage 3's dry assembly independently re-enforces both portfolio
freshness (`as_of` within `[decision_not_before -
maximum_portfolio_age_seconds, decision_deadline]`) and the exact
`len(open_listing_keys) == portfolio.open_positions` invariant, and the
genesis sealed in stage 2 always has zero open positions, a well-formed
first-session package request must always carry an **empty**
`open_listing_keys` tuple -- this packager does not need to (and does
not) re-implement that check itself; it is inherited unmodified from the
accepted assembly layer.

## Request schema

`promoted-paper-pilot-session-package-request/v1` -- strict UTF-8
canonical JSON, exact top-level and nested key sets, duplicate-key
rejection at every level, no floats/NaN/Infinity, literal integer typing,
canonical decimal strings, canonical UTC-Z timestamps, and one static
sanitized `PromotedPaperPilotSessionPackageError` boundary.

Fields: `research_run_id` (the only research input -- everything else is
derived), `target_session` (must equal the resolved preparation's own
target session), `expected_quote_source_id`, `open_listing_keys`,
`decision_not_before`/`decision_deadline`, `quote_gate_policy`
(the existing `LaunchQuoteGatePolicyRequest`), `allocation_policy` (the
existing `LaunchAllocationPolicyRequest`, itself nesting the existing
`LaunchSizingPolicyRequest`), `maximum_quote_chunk_size`,
`binding_bucket`.

This request **never** carries a caller-supplied preparation, portfolio,
policy, assembly, engine, graph, or operational-run ID -- every such
identity is derived exclusively from `research_run_id` plus the accepted
domain constructors this module composes.

## CLI

The CLI separately accepts:

- `--package-request-file` -- one session-package request, decoded with
  this module's own strict decoder.
- The ten explicit promoted-preparation roots (`--reference-root` through
  `--operational-preparation-root`, identical to
  `india-swing-promoted-operational-prepare`/
  `india-swing-promoted-operational-launch`).
- `--portfolio-artifact-root`.
- `--genesis-request-file` plus the four exact evidence files
  (`--broker-funds-file`, `--broker-positions-file`,
  `--engine-risk-ledger-file`, `--engine-pnl-ledger-file`) required by
  the already-accepted `promoted_paper_portfolio_genesis` boundary --
  decoded and archived through that boundary's own existing decoder/
  archive, never reproduced here.
- `--output-assembly-spec-file` -- the same create-once destination
  `india-swing-promoted-operational-job --assembly-spec-file` consumes.

All paths are explicit absolute non-traversing values. Input roots and
evidence are read-only except through the two exact accepted create-once
stores (`LocalPromotedOperationalPreparationStore`,
`LocalSwingPortfolioArtifactStore`/`LocalPromotedPortfolioEvidenceArchive`)
and the one exact create-once assembly-spec output writer -- this CLI
never overwrites, deletes, repairs, cleans, scans, or selects "latest".

## Dependency sanitization

Every accepted-dependency result the coordinator receives -- the
resolved preparation, the sealed portfolio artifact, the dry-assembled
runtime assembly and its assembly spec, the publish call, and the final
result construction -- is treated as untrusted at each of those five
stages: its exact type is checked before any property is read, its own
`verify_content_identity()` is called before any of its fields are
trusted, and every step is wrapped so that a wrong return type, a
malicious property, or any foreign exception from that dependency
collapses to the single static `PromotedPaperPilotSessionPackageError`
with no `__cause__`/`__context__` chain and no foreign text or value ever
included. `schema_version` on the package request itself is checked with
an exact `str` type test before the equality comparison, so a `str`
subclass whose value happens to equal the schema constant is still
rejected.

## Success envelope

One compact sorted JSON line: `status`
(`PROMOTED_PAPER_PILOT_SESSION_PACKAGE_READY`), `target_session`,
`research_run_id`, `preparation_id`, `portfolio_artifact_id`,
`portfolio_snapshot_id`, `assembly_spec_id`, `candidate_count`,
`open_position_count`, `paper_only=true`, `notification_eligible=false`,
`execution_eligible=false`. No path, capital amount, holding, evidence
hash, policy threshold, bucket name, candidate payload, secret, or
exception text is ever emitted, on success or on failure.

Failure produces no stdout, one static JSON stderr line
(`{"error_type": "PromotedPaperPilotSessionPackageError", "status":
"FAILED"}`), and exit code 2.

## Zero-candidate preparations

A promoted research run that selected zero candidates produces a
zero-candidate operational preparation -- this is already an accepted,
normal, auditable outcome of `prepare_and_publish` itself (never a
special case introduced here). This packager does not special-case
around it: if every already-accepted constructor in the composed chain
accepts a zero-candidate preparation, the resulting package is published
normally with `candidate_count=0`.

## Idempotent replay and conflicts

Running this packager twice with byte-identical package/genesis requests
and evidence against the same roots and the same output path succeeds
idempotently (each of the three composed stores/writers independently
accepts a byte-identical replay). Running it a second time with
*different* genesis/package inputs against the **same** output assembly-
spec-file path fails closed -- the create-once assembly-spec writer
rejects a differing payload at an already-occupied path, and no existing
artifact is overwritten, deleted, or repaired.

## Non-goals

This packager performs no deployment, no scheduling, no Telegram
delivery, no interactive/browser login, no token refresh, and grants no
real-capital authority -- every published record remains permanently
`paper_only=true` / `notification_eligible=false` /
`execution_eligible=false` on its own already-accepted type. It has no
environment, clock, GCP, Kite, network, LLM, or subprocess capability of
its own. The next commands remain the already-accepted
`india-swing-promoted-operational-cloud-control-prepare` and
`india-swing-promoted-operational-input-publish`; this increment
documents that exact handoff without running them.
