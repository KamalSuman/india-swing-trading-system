# Reviewed identity decisions and partial stable IDs

This package records explicit human review decisions against the identity
adjudication queue and materializes a partial stable-identity snapshot. It does
not choose the latest review, infer a decision from a ticker, or silently accept
an evidence claim.

## Safety boundary

Every decision must identify exactly one:

- persisted queue and source registry;
- candidate and required adjudication requirement;
- archived evidence artifact and claim; and
- explicit `ACCEPTED` or `REJECTED` outcome with rationale.

The materializer receives an explicit list of evidence and review bundle IDs.
Two decisions for the same candidate/requirement pair fail as a conflict; time
order is never used to choose one. A review cannot predate the evidence it cites
or be known after the requested cutoff.

The reviewer's ID and review time are self-declared. There is no cryptographic
signature or external reviewer authentication in this milestone. Therefore all
review bundles and identity snapshots remain `COLLECTION_ONLY` and
`actionable=false`, even when a stable ID is assigned.

## Review declaration

```json
{
  "schema_version": "identity-review-declaration/v1",
  "queue_id": "<exact-queue-id>",
  "source_registry_id": "<exact-registry-id>",
  "reviewer_id": "owner:kamal",
  "reviewed_at": "2026-07-16T21:15:00+05:30",
  "decisions": [
    {
      "candidate_id": "<exact-candidate-id>",
      "requirement": "OFFICIAL_LISTING_LIFECYCLE",
      "outcome": "ACCEPTED",
      "evidence_artifact_id": "<exact-evidence-artifact-id>",
      "evidence_claim_id": "<exact-claim-id>",
      "rationale": "Reviewed the named table row and effective date against the archived PDF."
    }
  ]
}
```

Unknown or duplicate JSON keys fail. A bundle cannot contain two decisions for
the same candidate/requirement pair.

## Stable-ID rules

A candidate receives an ID only when every required pair has one accepted
decision and none is rejected. A source-validated ISIN continues to be used
directly for an ordinary single-vintage or continuity candidate.

An unresolved source identifier can be corrected only when its queue requires
`VALIDATED_IDENTIFIER`, the accepted decision for that exact requirement cites
an evidence claim containing a syntactically valid ISIN, and the claim's symbol
and series exactly match the source observation. Every other accepted non-null
ISIN claim for the candidate must agree. A conflicted validated-ISIN candidate
can proceed only when its `OFFICIAL_CONFLICT_RESOLUTION` decision is accepted
and that exact evidence claim confirms the candidate's existing ISIN. Accepted
claims must refer to a symbol/series pair actually present in the candidate.
Unvalidated conflict shapes remain unsupported because a conflict review is not
itself a validated-identifier decision.

These rules do not infer an ISIN from a ticker, a provider catalog, declaration
order, or an unreviewed claim. Missing, rejected, mismatched, or contradictory
evidence fails closed.

The stable instrument ID is derived from NSE CM plus the validated ISIN. A
stable listing ID is derived from that instrument ID plus the NSE series. A
symbol rename in the same series therefore preserves the listing ID. A series
change creates a separate listing ID.

The output stores one effective observation on each security-master report date
actually present in the registry. It deliberately does not claim that the first
observed date is the legal listing date, fill gaps between vintages, or extend a
mapping before/after observed coverage. An ISIN-changing corporate action is
also outside this version and will create a different instrument ID until a
future cross-ISIN continuity contract is implemented.

## Commands

```powershell
$env:PYTHONPATH = "src"

python -m india_swing.identity_decisions.cli review-import `
  --declaration C:\path\to\review.identity.json

python -m india_swing.identity_decisions.cli review-show `
  --review-bundle-id <bundle-id>

python -m india_swing.identity_decisions.cli materialize `
  --registry-id <registry-id> `
  --evidence-id <explicit-evidence-id> `
  --review-bundle-id <explicit-review-bundle-id> `
  --cutoff <ISO-8601-cutoff>

python -m india_swing.identity_decisions.cli snapshot-show `
  --snapshot-id <snapshot-id>
```

`--evidence-id` and `--review-bundle-id` may be repeated. Omitting both is
valid and produces a sealed snapshot in which every candidate is explicitly
blocked by `MISSING_REVIEW_DECISION`.

The review and snapshot stores share `INDIA_SWING_IDENTITY_EVIDENCE_ROOT`,
defaulting to `var/identity_evidence`.
