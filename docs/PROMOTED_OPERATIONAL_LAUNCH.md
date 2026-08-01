# Promoted operational launch-spec preparer

`src/india_swing/promoted_operational_launch.py` (pure preparer) and
`src/india_swing/promoted_operational_launch_cli.py` (offline CLI) turn
one strict, human-authored launch request plus one exact promoted
preparation and one exact portfolio artifact into the canonical
`PromotedOperationalAssemblySpec` file that
`india-swing-promoted-operational-job`'s `--assembly-spec-file` option
consumes (see
[`docs/PROMOTED_OPERATIONAL_ASSEMBLY.md`](PROMOTED_OPERATIONAL_ASSEMBLY.md)
and [`docs/PROMOTED_OPERATIONAL_JOB.md`](PROMOTED_OPERATIONAL_JOB.md)).
This closes the operator gap the job entrypoint intentionally left open:
it never invents, discovers, or infers a preparation or portfolio
artifact -- an operator supplies both exact IDs, and every other value in
the produced spec is either an explicit launch control or independently
derived through the already-accepted constructors.

Console script: `india-swing-promoted-operational-launch` (declared in
`pyproject.toml`, mapped to
`india_swing.promoted_operational_launch_cli:main`).

**This preparer is entirely offline: no clock, environment variable,
credential, or Kite/GCP/Telegram/network/broker/runtime/deployment
capability exists anywhere in either module. It never executes the
promoted engine and never places an order.**

## Launch request schema

The request is one strict JSON object,
`schema_version="promoted-operational-launch-request/v1"`, with exactly
these top-level keys -- no optional or default field exists:

```jsonc
{
  "schema_version": "promoted-operational-launch-request/v1",
  "preparation_id": "<64-hex lowercase sha256>",
  "portfolio_artifact_id": "<64-hex lowercase sha256>",
  "expected_quote_source_id": "<64-hex lowercase sha256>",
  "open_listing_keys": ["NSE:EXAMPLE"],
  "decision_not_before": "2026-01-01T09:15:00Z",
  "decision_deadline": "2026-01-01T15:00:00Z",
  "quote_gate_policy": {
    "maximum_batch_collection_seconds": 15,
    "maximum_quote_age_seconds": 15,
    "maximum_last_trade_age_seconds": 300,
    "maximum_spread_bps": "50"
  },
  "allocation_policy": {
    "maximum_portfolio_age_seconds": 300,
    "sizing_policy": {
      "per_trade_risk_fraction": "0.005",
      "maximum_total_open_risk_fraction": "0.02",
      "maximum_position_notional_fraction": "0.25",
      "maximum_gross_exposure_fraction": "0.80",
      "maximum_daily_turnover_participation": "0.0025",
      "maximum_top_ask_participation": "0.20",
      "maximum_daily_loss_fraction": "0.01",
      "maximum_pilot_drawdown_fraction": "0.02",
      "minimum_net_reward_risk": "2.50",
      "maximum_open_positions": 4,
      "maximum_new_positions_per_run": 1
    }
  },
  "maximum_quote_chunk_size": 500,
  "binding_bucket": "<placeholder-bucket-name>"
}
```

Every value above is a **placeholder** -- no real ID, path, bucket,
credential, or portfolio value belongs in this document or in a checked-in
example. Decimal fields are canonical strings (`str(Decimal(value)) ==
value`, so `"50"` is accepted but `"50.00"` or `"5E1"` is rejected as
noncanonical); integer fields are literal JSON integers (a boolean is
never accepted where an integer is required); timestamps are literal
UTC ISO-8601 strings with a trailing `Z` (never `+00:00` or another
offset) that round-trip byte-for-byte; `open_listing_keys` must already
be the canonical (sorted, unique, `NSE:...`-shaped) tuple.

**Policy threshold values are deliberate launch controls, never inferred
from market data or candidates.** The request supplies every quote-gate,
sizing, portfolio-age, and chunk value explicitly. The request never
carries a caller-supplied policy ID, policy version, authority flag,
target session, portfolio snapshot ID, or assembly spec ID -- every one
of those is derived and independently replayed from the already-accepted
constructors and resolved parents; a request could not smuggle in a
different value for any of them even if it tried.

## Strict decoding

`decode_promoted_operational_launch_request` reads the request bytes
using `read_stable_regular_file` with an immutable 64 KiB maximum,
requires strict UTF-8, requires exactly one JSON object, rejects
duplicate keys at **every** nesting level (top level, `quote_gate_policy`,
`allocation_policy`, and its nested `sizing_policy`), requires exact key
sets at every level, rejects floats/NaN/Infinity/other JSON constants
anywhere in the payload, rejects a boolean where an integer is required,
requires canonical decimal strings, requires canonical `Z`-suffixed UTC
timestamps that round-trip exactly, and requires `decision_not_before <
decision_deadline`. Every rejection collapses to the one static
`PromotedOperationalLaunchError`, never chained, with `__cause__` and
`__context__` both `None`.

## Exact resolution and mandatory dry assembly

0. `request.replay()` is called **first**, inside a sanitized boundary,
   before `request.preparation_id` is ever read or either resolver is
   ever called. Every nested request type (`LaunchQuoteGatePolicyRequest`,
   `LaunchSizingPolicyRequest`, `LaunchAllocationPolicyRequest`,
   `PromotedOperationalLaunchRequest`) carries its own `replay()` that
   reconstructs a fresh exact instance from every retained field, reruns
   `__post_init__`, and requires equality; the top-level replay calls
   both nested replays first. This catches any post-construction
   `object.__setattr__` tamper of any field anywhere in the request
   graph -- including a nested request object replaced outright -- and
   leaves both durable resolvers uncalled on rejection.
1. `preparation_resolver.get(preparation_id)` is called **exactly once**.
   Wrong type, an exception, tampered content, or a requested/returned ID
   mismatch all fail closed -- **before** the portfolio resolver is ever
   called.
2. After the preparation is verified, its `manifest.target_session` is
   read inside a sanitized boundary, and the request's
   `decision_not_before`/`decision_deadline` are each converted with
   `.astimezone(INDIA_STANDARD_TIME).date()` and required to equal that
   exact `target_session`. Any mismatch or exception fails closed
   **before** the portfolio resolver is ever called -- a decision window
   for a different Indian market session can never reach the portfolio
   read.
3. `portfolio_artifact_resolver.get(portfolio_artifact_id)` is then
   called **exactly once**, with the identical fail-closed treatment,
   before the spec is ever constructed.
4. `target_session` is derived only from the verified preparation's own
   `manifest.target_session`; `expected_portfolio_snapshot_id` is derived
   only from the verified artifact's own `portfolio_snapshot_id` --
   never from the request.
5. `SwingQuoteGatePolicy`, `SwingPortfolioSizingPolicy`, and
   `PromotedOperationalAllocationPolicy` are constructed from the
   request's explicit scalars; their own accepted constructors derive
   the versioned `policy_id`/`allocation_policy_id` -- never copied from
   the request.
6. `PromotedOperationalAssemblySpec` is constructed with fixed
   `paper_only=True`/`notification_eligible=False`/
   `execution_eligible=False`.
7. The **mandatory dry assembly** -- `assemble_promoted_operational_runtime_inputs`
   (unchanged) -- is called **exactly once**, using small in-memory
   resolver wrappers pinned to the two already-resolved exact parent
   objects. Each pinned wrapper independently re-verifies its retained
   parent's content identity and requires the requested ID to exactly
   equal the parent's own freshly-verified ID before returning it --
   defense-in-depth even though the dry assembly re-verifies parents
   internally. This never re-reads either durable store, and it must
   pass before any output is published: every parent-lineage,
   session/window, portfolio-freshness/open-position, source-ID, policy,
   and paper-only check the accepted assembly layer already enforces
   runs again here, independently, before this preparer ever writes a
   file.

## Create-once output

`publish_promoted_operational_launch_assembly_spec_file` first requires,
inside a sanitized boundary and **before any path inspection or
encoding**, that `spec` is exactly `PromotedOperationalAssemblySpec` and
that `spec.verify_content_identity()` succeeds; encoding itself is also
performed inside its own sanitized boundary. Every failure here --
wrong type, a tampered/stale spec, or an encoder failure -- raises only
the static `PromotedOperationalLaunchError` with `__cause__`/`__context__`
both `None`, never the foreign `PromotedOperationalAssemblyError` the
encoder itself would otherwise raise, and never touches the filesystem.

The function then writes the encoded spec to one explicit, concrete,
absolute, non-traversing `--output-assembly-spec-file` whose **parent
directory must already exist** as a real, non-link directory -- this
function never creates or scans a directory. It never truncates,
overwrites, replaces, or deletes:

- An absent target is created **exclusively** (`O_CREAT | O_EXCL`),
  fully written, `fsync`'d, and closed, then cold-read back via
  `read_stable_regular_file` and required byte-identical to what was
  just written. The cold bytes are then also decoded with
  `decode_promoted_operational_assembly_spec`, and the decoded
  `assembly_spec_id` is required to equal the supplied verified spec's
  ID before the call returns successfully.
- An existing exact regular file is accepted **only** if it is
  byte-identical to the freshly encoded payload (a repeat invocation with
  the same request is therefore idempotent) **and**, once byte-identical,
  cold-decodes to the same `assembly_spec_id` as the supplied verified
  spec. Any other existing content -- different, empty, oversized, a
  symlink/reparse point, or otherwise unsafe -- fails closed and is left
  completely unmodified.
- No temporary-file rename is ever used, since that pattern can silently
  replace an existing file; the exclusive-create-then-cold-read sequence
  above cannot.
- **Partial-write recovery is manual.** If the exclusive create succeeds
  but a later step in the same write (for example a partial `write` or a
  failed `fsync`) fails before the file is verified, the on-disk bytes
  written so far are left exactly as they are -- this function never
  deletes, truncates, or repairs a file it created. Because the existing
  file is neither absent nor byte-identical to a fresh encoding, every
  subsequent retry with the same or a different spec also fails closed
  against that same poisoned path rather than silently overwriting it.
  An operator must manually remove the poisoned file before a retry to
  that path can succeed.

## CLI command shape

```text
india-swing-promoted-operational-launch \
  --launch-request-file /abs/path/to/request.json \
  --reference-root /abs/path/to/reference \
  --identity-evidence-root /abs/path/to/identity-evidence \
  --calendar-root /abs/path/to/calendar \
  --daily-reports-root /abs/path/to/daily-reports \
  --historical-corpus-root /abs/path/to/historical-corpus \
  --promoted-root /abs/path/to/promoted \
  --graph-publication-root /abs/path/to/graph-publication \
  --engine-run-root /abs/path/to/engine-run \
  --research-run-root /abs/path/to/research-run \
  --operational-preparation-root /abs/path/to/operational-preparation \
  --portfolio-artifact-root /abs/path/to/portfolio-artifact \
  --output-assembly-spec-file /abs/path/to/assembly.json
```

Every option is required exactly once; unknown, duplicate, or missing
options fail closed. Every value must be a concrete, absolute,
non-traversing path. The CLI never inspects an environment variable or
the current time.

## Success and failure envelopes

On success, the CLI prints exactly one compact, sorted-key,
`allow_nan=false` JSON object to stdout (nothing to stderr) with exactly:

```text
status, assembly_spec_id, preparation_id, portfolio_artifact_id,
portfolio_snapshot_id, target_session, decision_not_before,
decision_deadline, quote_gate_policy_id, allocation_policy_id,
expected_quote_source_id, candidate_count, open_position_count,
paper_only, notification_eligible, execution_eligible
```

`status` is always `"PROMOTED_OPERATIONAL_LAUNCH_READY"`.
`candidate_count`/`open_position_count` are derived from the verified
resolved parents; timestamps are canonical UTC `Z` strings. No path,
bucket, holding, policy threshold, candidate/listing key, the raw
request, or exception text is ever printed.

Any failure -- argument, filesystem, decoding, policy construction,
resolver, dry-assembly, or publication -- writes no stdout, prints
exactly `{"error_type":"PromotedOperationalLaunchError","status":"FAILED"}`
to stderr, and returns exit code 2.

## Feeding the produced file into the job entrypoint

The file this preparer publishes is exactly the file
`india-swing-promoted-operational-job --assembly-spec-file` expects --
`load_promoted_operational_assembly_spec_file` (unchanged) reads it
directly. This preparer and the job entrypoint are two separate,
independently reviewed processes; running this preparer never executes
the job, and the job never re-derives anything this preparer already
proved via the mandatory dry assembly.

## Non-goals

- No clock, environment variable, or credential is ever read.
- No Kite/GCP/Telegram/network/broker/runtime client is ever constructed.
- No candidate reranking, price mutation, policy inference, or
  open-listing inference -- every policy threshold is an explicit launch
  control, and `open_listing_keys` is explicit evidence, never derived.
- No filesystem discovery, listing, or "latest" selection anywhere.
- No overwrite, replace, delete, or repair of an existing output file.
- No notification or order-execution authority is introduced; every
  produced spec remains `paper_only=True`.
