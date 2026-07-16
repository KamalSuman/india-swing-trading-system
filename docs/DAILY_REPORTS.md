# NSE daily multiple-report bundle

Status: a strict collection-only parser, deterministic normalized codec,
create-once local artifact store, and CLI are implemented for NSE's manually
downloaded `Reports-Daily-Multiple.zip`. Imported bytes are ignored by Git and no
bundle artifact is committed to the repository.

## Import

Keep the portal ZIP intact:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.daily_reports.cli bundle import `
  --file C:\path\to\Reports-Daily-Multiple.zip
```

The default archive root is `var/daily_reports`; override it with
`INDIA_SWING_DAILY_REPORTS_ROOT`. The command needs no Kite credentials and emits
only sanitized error types on failure.

The store accepts the exact official outer basename, preserves the exact outer
ZIP, a deterministic normalized payload, and a content-bound manifest. The
source is opened once and checked by file identity before and after reading. ZIP
CRCs, entry paths, duplicate names, encryption, expanded sizes, report headers,
row widths, identifiers, dates, duplicate keys, and cross-report invariants are
checked before durable atomic publication. Process-released advisory locks make
a retry safe after a crash. Every read re-hashes and re-parses the artifact.

## Approved report families and date roles

| Report | Validation | Date interpretation |
|---|---|---|
| `BhavCopy_NSE_CM_..._YYYYMMDD_F_0000.csv.zip` | Exact UDiFF header, one inner CSV, CM/NSE/STK/F1 scope, validated ISIN, unique dated IDs/listings, positive and consistent OHLCV, and traded-value average inside the daily low/high range | Filename date must equal every `TradDt` and `BizDt`; confirmed trade date |
| `sec_bhavdata_full_DDMMYYYY.csv` | Exact header, delivery arithmetic, and exact reconciliation to same-date UDiFF OHLC, previous/last/close, volume, trades, turnover and average price; complete row coverage for every series it reports and mandatory full coverage of UDiFF `EQ` | Filename date must equal every `DATE1`; confirmed trade date |
| `REG1_INDDDMMYY.csv` | Exact 63-column schema, unique `(Symbol, Series)`, independent status/GSM/ASM/ESM and indicator domains, blank fillers | Filename is a claim for the after-close publication session; effective state is the next verified NSE session, which requires the calendar |
| `sec_list_DDMMYYYY.csv` | Exact complete-price-band schema and unique listing keys | Filename is a publication/as-of claim; the contents apply to the next verified trading session. This file includes SME rows and is not “main-board only” |
| `sme_bands_complete_DDMMYYYY.csv` | Exact SME subset schema and unique listing keys | Filename is a claimed effective date |
| `eq_band_changes_DDMMYYYY.csv` | Exact schema, contiguous serials, unique listing keys and real band transitions | Filename is a claimed effective date |
| `series_change.csv` | Exact schema, valid internal dates and one transition per symbol/effective date | `Change Date` is an internal effective date; the mutable filename supplies no historical publication time |

Only these families are normalized. Every other outer entry remains preserved in
the raw ZIP and receives `IGNORED_UNAPPROVED`. The selected set must contain all
seven families, and UDiFF/full-delivery date coverage must agree.

The full-delivery report's known core scope is pinned to `EQ` because this is an
equity swing system. The parser also rejects a missing row inside any series that
the full report does contain. It does not infer that every non-`EQ` UDiFF series
must appear in the full file: debt, auction, block, and other series legitimately
appear only in UDiFF. A future actionable materializer must pin any additional
eligible series explicitly rather than learning full-file scope from the file it
is validating.

## Interoperability quarantine

NSE lists two MII security reports with the same downloaded basename. A bundle
can therefore contain `NSE_CM_security_DDMMYYYY.csv.gz` from the **NSE Listed and
BSE Exclusive securities** report. If any row has `PrtdToTrad=2`, the bundle
parser validates the gzip/header/row shape, records a summary, and marks it
`QUARANTINED_INTEROPERABILITY_SECURITY_MASTER`. Its rows are never normalized
into the NSE universe.

An NSE-only security master is marked `DEFERRED_NSE_ONLY_SECURITY_MASTER` and
must pass through the dedicated reference-data importer. The bundle layer never
silently filters one venue out of a mixed master.

## Bias and identity boundaries

- A UDiFF `FinInstrmId` is a dated listing/session identifier, not a permanent
  company identity. Series changes can replace it while ISIN continuity remains.
- A valid ISIN helps cross-vintage reconciliation but does not by itself prove
  listing eligibility, board, knowledge time, or corporate-action treatment.
- REG1 dimensions remain independent. `Status=S`, ESM, GSM, ASM, IRP and other
  flags are preserved; none is collapsed into `NONE` or a single permissive enum.
- `sec_list` and the SME complete file overlap. A future materializer must use a
  verified calendar and reconcile them into one state rather than appending rows.
- `series_change.csv` is a latest-change report, not a complete future schedule.
- Filename-only dates are claims. Local `first_seen_at` is knowledge time and is
  never backdated to a filename or row-effective date.
- The imported artifact is always `COLLECTION_ONLY`, `actionable=false`, and
  `UNVERIFIED_MANUAL_FILE`. No current pipeline path can turn it into an alert.

## Still required before materialization

The next layer needs a verified NSE calendar, a separately archived NSE-only
security master, explicit report acquisition evidence, corporate actions,
historical listing/series transitions, and a policy that blocks every unsupported
surveillance state. Until those inputs agree at a cutoff, the daily bundle is
evidence collection—not a point-in-time universe or trading recommendation.
