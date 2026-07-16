# Identity and lifecycle evidence archive

This package archives exact official NSE source bytes plus a strict manual JSON
declaration. It supports two source families:

- `LISTING_CIRCULAR_PDF` for Listing Department circulars; and
- `CORPORATE_ACTION_CSV` for the NSE corporate-action download.

The archive is deliberately collection-only. A claim means only that a reviewer
mapped one source locator to one candidate/requirement pair. It does not prove
the interpretation, satisfy the requirement, resolve the case, assign a stable
identity, or make any downstream artifact actionable.

## Declaration contract

Every declaration binds the exact source filename, byte count, and SHA-256.
Unknown or duplicate JSON keys fail. Source URLs must use an official NSE host.
PDF claims require a positive `page` and no row; CSV claims require a positive
`row` and no page. Listing-lifecycle and listing-status claims require an
explicit effective date.

Example PDF declaration:

```json
{
  "schema_version": "nse-cm-identity-evidence-declaration/v1",
  "exchange": "NSE",
  "segment": "CM",
  "claimed_authority": "NSE",
  "source_kind": "LISTING_CIRCULAR_PDF",
  "claimed_document_id": "NSE/LIST/C/2026/0489",
  "claimed_issue_date": "2026-03-23",
  "claimed_publication_at": null,
  "claimed_source_url": "https://nsearchives.nseindia.com/content/circulars/CML73417.pdf",
  "source_filename": "CML73417.pdf",
  "source_media_type": "application/pdf",
  "source_byte_count": 268719,
  "source_sha256": "<sha256-of-exact-pdf>",
  "claims": [
    {
      "candidate_id": "<exact-candidate-id-from-the-selected-queue>",
      "requirement": "OFFICIAL_LISTING_LIFECYCLE",
      "effective_date": "2026-03-24",
      "symbol": "<symbol-shown-in-source>",
      "series": "EQ",
      "isin": null,
      "locator": {
        "page": 1,
        "row": null,
        "section": "Removal table"
      },
      "claim_text": "<short reviewer transcription of the relevant statement>"
    }
  ]
}
```

`claimed_issue_date` and `claimed_publication_at` remain unverified source
claims. The manifest's UTC `validated_at` is the earliest local knowledge time
supported by this archive.

For a corporate-action CSV, use `CORPORATE_ACTION_CSV`, `text/csv`, a `.csv`
filename, and a row locator. The CSV header must include `SYMBOL`, `COMPANY
NAME`, `SERIES`, `PURPOSE`, and `EX-DATE`. Gzip files are not accepted by this
version; preserve/download the uncompressed CSV.

## Commands

```powershell
$env:PYTHONPATH = "src"

python -m india_swing.identity_evidence.cli import `
  --source C:\path\to\official-source.pdf `
  --declaration C:\path\to\official-source.identity.json

python -m india_swing.identity_evidence.cli show `
  --evidence-id <evidence-artifact-id>

python -m india_swing.identity_evidence.cli list

python -m india_swing.identity_evidence.cli coverage `
  --registry-id <registry-id> `
  --evidence-id <evidence-artifact-id>
```

`coverage` rejects claims that do not map to an exact requirement in the
selected persisted queue. Its `evidence_collected_pair_count` is presence only;
`requirements_satisfied`, `actionable`, and `stable_identity_assigned` always
remain false in this milestone.

The default archive root is `var/identity_evidence`. Override it with
`INDIA_SWING_IDENTITY_EVIDENCE_ROOT`.
