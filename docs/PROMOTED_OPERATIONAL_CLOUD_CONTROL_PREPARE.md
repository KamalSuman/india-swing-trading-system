# Promoted operational cloud-control preparation

`india-swing-promoted-operational-cloud-control-prepare` is the offline,
operator-side bridge between an accepted promoted-operational assembly spec and
the existing input-snapshot publisher.

It does not ask the operator to calculate an operational-run-spec ID. It loads
the exact assembly, resolves the assembly's exact preparation and portfolio
artifact from the eleven explicit read-only roots, and calls the accepted
`assemble_promoted_operational_runtime_inputs` boundary. The resulting run-spec
ID is therefore derived by the same deterministic implementation used by the
paper engine.

The command performs no GCS, Kite, Telegram, broker, environment, clock,
deployment, or scheduling operation.

## First-run command

All paths must be absolute, non-traversing paths. The eleven input roots and
the output file's parent must already exist as real, non-link directories.
`state-root` is bound into the control but is deliberately never inspected or
created by this command.

```powershell
& .\.venv\Scripts\python.exe -m india_swing.promoted_operational_cloud_control_prepare_cli `
  --assembly-spec-file C:\absolute\run\assembly-spec.json `
  --reference-root C:\absolute\data\reference_root `
  --identity-evidence-root C:\absolute\data\identity_evidence_root `
  --calendar-root C:\absolute\data\calendar_root `
  --daily-reports-root C:\absolute\data\daily_reports_root `
  --historical-corpus-root C:\absolute\data\historical_corpus_root `
  --promoted-root C:\absolute\data\promoted_root `
  --graph-publication-root C:\absolute\data\graph_publication_root `
  --engine-run-root C:\absolute\data\engine_run_root `
  --research-run-root C:\absolute\data\research_run_root `
  --operational-preparation-root C:\absolute\data\operational_preparation_root `
  --portfolio-artifact-root C:\absolute\data\portfolio_artifact_root `
  --state-root C:\absolute\run\state `
  --output-control-file C:\absolute\run\cloud-control.json
```

Success emits one compact `PROMOTED_OPERATIONAL_CLOUD_CONTROL_READY` JSON line
containing the derived assembly, operational-run, runtime-job, preparation,
portfolio, and control identities plus paper-only authority flags. It does not
emit local paths.

The output is create-once. A byte-identical existing control is accepted as an
idempotent replay; divergent, linked, non-regular, replaced-parent, or unsafe
output fails closed and is never overwritten or repaired. The output file may
not overlap the assembly file, any input root, or `state-root`.

## Same-run restart

The optional restart coordinates are for restarting the *same exact run spec*,
not for selecting a previous trading day or discovering a latest object. Supply
all three fields from a previous hydrated-cloud success envelope, or supply
none:

```powershell
  --prior-state-manifest-object-name promoted-operational-state/v1/2026-08-11/<operational-run-spec-id>/manifests/<publication-id>.json `
  --prior-state-manifest-generation 7 `
  --prior-state-manifest-sha256 <64-lowercase-hex>
```

The accepted `PromotedOperationalGCSRestoreRequest` constructor independently
checks the object path, generation, hash, and derived current operational spec.
A partial group, foreign session/spec path, noncanonical generation, or invalid
hash fails closed.

## Next command

After preparation succeeds, publish the immutable input snapshot and portable
launch file with the existing command:

```powershell
& .\.venv\Scripts\python.exe -m india_swing.promoted_operational_input_publish_cli `
  --source-control-file C:\absolute\run\cloud-control.json `
  --output-launch-file C:\absolute\run\hydrated-launch.json
```

That second command performs the GCS publication. The control preparer itself
remains fully offline.
