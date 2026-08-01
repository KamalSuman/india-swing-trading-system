# Promoted paper portfolio genesis seal

`india-swing-promoted-portfolio-genesis` creates the initial empty INR paper
portfolio required by the promoted operational launch. It is an offline,
manual-reconciliation boundary. It does not log in to Kite, query a broker,
call GCP, send Telegram, place an order, or grant live-capital authority.

The tool deliberately cannot create arbitrary portfolio state. The request
supplies only positive capital and an explicit UTC `as_of`; the resulting
portfolio always has cash equal to capital and zero exposure, open risk, open
positions, daily P&L, and pilot P&L. Later state changes belong to the paper
outcome and daily paper-portfolio workflow.

## Request

The request is strict canonical JSON (maximum 32 KiB):

```json
{"as_of":"2026-08-03T03:44:00Z","capital":"100000","evidence":[{"expected_sha256":"<broker-funds-sha256>","kind":"BROKER_FUNDS","observed_at":"2026-08-03T03:43:50Z","source_version":"manual-paper-reconciliation/v1"},{"expected_sha256":"<broker-positions-sha256>","kind":"BROKER_POSITIONS","observed_at":"2026-08-03T03:43:51Z","source_version":"manual-paper-reconciliation/v1"},{"expected_sha256":"<engine-risk-ledger-sha256>","kind":"ENGINE_RISK_LEDGER","observed_at":"2026-08-03T03:43:52Z","source_version":"manual-paper-reconciliation/v1"},{"expected_sha256":"<engine-pnl-ledger-sha256>","kind":"ENGINE_PNL_LEDGER","observed_at":"2026-08-03T03:43:53Z","source_version":"manual-paper-reconciliation/v1"}],"manual_reconciliation_ack":"I_HAVE_MANUALLY_RECONCILED_THE_FOUR_EVIDENCE_FILES_FOR_PAPER_ONLY_USE","schema_version":"promoted-paper-portfolio-genesis-request/v1"}
```

Evidence must appear in exactly the shown order. Each `observed_at` must be UTC
and no later than `as_of`. The acknowledgement means only that the human
operator compared the four files and accepts them for paper research. It is
not a claim of automated broker verification.

Compute each raw-file hash in PowerShell without changing the file:

```powershell
(Get-FileHash -Algorithm SHA256 -LiteralPath C:\absolute\evidence-file).Hash.ToLowerInvariant()
```

Each evidence file must be a stable, non-empty regular file no larger than
8 MiB. Keep broker exports and statements outside Git.

## Invocation

```powershell
india-swing-promoted-portfolio-genesis `
  --request-file C:\absolute\genesis-request.json `
  --portfolio-artifact-root C:\absolute\promoted-portfolio `
  --broker-funds-file C:\absolute\broker-funds-export `
  --broker-positions-file C:\absolute\broker-positions-export `
  --engine-risk-ledger-file C:\absolute\empty-risk-ledger.json `
  --engine-pnl-ledger-file C:\absolute\empty-pnl-ledger.json
```

Create the evidence and run this command immediately before the chosen paper
decision window. The operational launch independently enforces its portfolio
freshness ceiling, so an old genesis artifact will fail closed.

## Storage and retries

Raw evidence is archived create-once under
`<portfolio-artifact-root>/reconciliation_evidence/<KIND>/<sha256>.bin`.
Only after all four archives succeed is the accepted portfolio artifact
published under `<portfolio-artifact-root>/portfolio_snapshots/`. The artifact
is the final commit marker.

An identical retry is idempotent. A crash may leave a canonical subset of raw
evidence archives but cannot publish a completed portfolio artifact early.
Existing conflicting, truncated, linked, or otherwise unsafe evidence targets
are never overwritten, deleted, or repaired; the command fails closed and an
operator must inspect and remediate the poisoned path manually.

Success prints IDs and authority flags only. The stored artifact remains
`MANUAL_RECONCILED_PAPER_ONLY` and `PAPER_ONLY`; it is not notification- or
execution-eligible.
