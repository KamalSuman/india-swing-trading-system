# NSE reference-data boundary

Status: contracts, synthetic tests, and a collection-only NSE CM MII security
master importer are implemented. No imported market-data artifact is committed
to the repository, and no surveillance file, holiday circular, or live universe
has been materialized.

The pipeline now requires content-addressed calendar and universe artifacts. A
current Kite instrument dump remains inventory-only and cannot be labelled
point-in-time verified or become actionable.

## Official source map

The safest free starting point for dated NSE cash-market membership is the daily
**CM - MII - Security File (.gz) (NSE Listed securities)** on
[NSE All Reports](https://www.nseindia.com/all-reports). Its dated filename is
`NSE_CM_security_DDMMYYYY.csv.gz`. NSE introduced this dissemination in
[circular MSD60315](https://nsearchives.nseindia.com/content/circulars/MSD60315.pdf)
from 5 February 2024. The current field contract is described in the
[NSE Masters Data specification](https://nsearchives.nseindia.com/web/mediaattachment/2026-04/NSE-Masters_Data-v1.8_20260428121249.pdf).
The exact 120-column ISO-tag CSV order is specified in Annexure 10 of PART-D in
[NSE capital-market consolidated circular CMTR73927](https://nsearchives.nseindia.com/content/circulars/CMTR73927.zip).

The report catalogue shows the human-facing display name rather than the
downloaded filename. Search for `MII` or `Security File`, select the dated
**NSE Listed securities** entry, and keep the `.csv.gz` compressed. Do not select
the separate **NSE Listed and BSE Exclusive securities** interoperability entry.

## Implemented manual import boundary

The reference-data CLI accepts exactly one manually downloaded NSE-only file:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.reference_data.cli security-master import `
  --file C:\path\to\NSE_CM_security_DDMMYYYY.csv.gz
```

The importer does not download or scrape anything. It:

- opens one regular non-link file descriptor, verifies its identity before and
  after the bounded read, and rejects path swaps or concurrent mutation;
- rejects corrupt, concatenated, trailing, non-UTF-8, or unknown-schema gzip data;
- pins the current 120-column ISO-tag header and exact dated filename pattern;
- validates every row and rejects duplicate instrument IDs or symbol-series keys;
- rejects interoperability content carrying BSE-exclusive alternative-venue rows;
- rejects nonblank values in ISO scope/type fields that are blank under the
  currently pinned NSE cash-market schema;
- preserves every source field and assigns exactly one auditable row disposition;
- preserves the raw source identifier while exposing an ISIN only when its
  12-character structure and check digit validate;
- stores the original bytes, deterministic normalized JSON, hashes, row digest,
  parser/schema/policy versions, and internally observed availability times;
- durably publishes the local artifact atomically under a process-released
  advisory lock and verifies it again on every read;
- remains `COLLECTION_ONLY` and `actionable=false`.

The filename date is stored as `claimed_report_date`, never as historical
knowledge time. The CSV has no internal report-date control row, so a local file
cannot independently prove that date or its origin. Its acquisition mode is
therefore `UNVERIFIED_MANUAL_FILE`, `verified_report_date` remains null, and
freshness selection refuses to use the filename claim. The archive is
partitioned by successful validation date. The manual public channel rejects
claimed dates before 5 February 2024 and implausibly far-future filenames.
Re-importing identical content keeps the earliest stored artifact; conflicting
bytes for the same claimed date fail closed under an atomic per-date import lock.
An authorized downloader or acquisition receipt must establish source URL,
retrieval evidence, and a verified report date before point-in-time promotion.

NSE master date/time integers use NSE's documented epoch of 1 January 1980, not
the Unix epoch. The importer deliberately preserves them as raw integers. It
also treats `BidIntrvl` as the paise-denominated tick field and does not substitute
the currently reserved ISO-tag `TickSz` column.

Daily surveillance/regulatory enrichment comes from `REG1_INDDDMMYY.csv` (and
the older `REG_INDDDMMYY.csv`). The consolidated REG1 file is generated after
market close and applies to the **next trading session**; that knowledge/effective
distinction is mandatory. Relevant official schema circulars include
[SURV64924](https://nsearchives.nseindia.com/content/circulars/SURV64924.zip),
[SURV65097](https://nsearchives.nseindia.com/content/circulars/SURV65097.zip),
and [SURV67801](https://nsearchives.nseindia.com/content/circulars/SURV67801.pdf).

The final UDiFF bhavcopy provides prices and trading evidence. It is not a
security master: an eligible zero-volume or suspended security can be absent, so
using bhavcopy membership as the universe would create survivorship bias.

Mutable current files such as `EQUITY_L.csv`, `SME_EQUITY_L.csv`, current
symbol/name-change CSVs, today's Kite instruments, and current ASM/GSM pages are
useful for validation but cannot be projected backward.

## Trusted acquisition-receipt verification boundary

`src/india_swing/reference_data/acquisition_receipt.py` implements a pure,
externally pinned verifier (`ReferenceAcquisitionReceiptVerifier`) for one
authorized NSE CM MII security-master acquisition receipt. It follows the same
trust pattern as `LandingManifestVerifier`: a `TrustedReferenceAcquisitionBinding`
supplies the only source of trust (expected receipt/raw SHA-256 hashes, the
allowed GCS bucket, the exact report date, aware-UTC `not_before`/`cutoff`
bounds, and the trusted acquirer ID), all caller-supplied and never inferred
or recomputed from receipt content, an environment variable, a clock, GCS, a
filename, or a network response. The verifier performs no network, GCS,
filesystem, environment, clock, listing, or "latest" access; it accepts
strict UTF-8 JSON receipt bytes (at most 64 KiB) and fails closed with one
static, sanitized `ReferenceAcquisitionReceiptError` that never echoes
receipt content, a URL, a bucket or object name, a hash, an acquirer ID,
nested exception text, a path, or credential-like text.

**A verified receipt is necessary but not sufficient.** It proves only that
one exact claimed acquisition record — source, acquirer, acquisition time
window, claimed download URL, HTTP status/media type, raw byte count, raw
SHA-256, and the pinned GCS landing object (bucket/object name/generation) —
matches an independently governed hash and bound. It does **not** download
anything, does not read the landing object's actual bytes, does not parse the
security master, and does not promote any artifact's readiness. Code that
later joins a verified receipt to the acquired raw bytes and the existing
collection artifact must still:

- exact-generation-read the pinned landing object from GCS (never a listing
  or "latest" read), independently re-verifying its generation and content
  hash against the receipt's own `raw_sha256`, exactly as
  `GCSLandingObjectReader.read` already does for `LandingObjectRequest`;
- recompute the parse/normalization semantics against the actual downloaded
  bytes (mirroring how the manual importer independently re-derives every
  manifest field from re-parsed bytes rather than trusting a filename or
  external claim); and
- explicitly, separately decide to promote the resulting joined artifact's
  `AcquisitionMode`/readiness/`verified_report_date` — a verified receipt by
  itself changes none of those fields on any existing artifact.

A receipt generated from an unauthenticated manual claim, or whose own
internal hash was computed from the same untrusted receipt bytes it is meant
to authenticate, is self-consistent but is **not** independent provenance —
exactly the same failure mode `LandingManifestVerifier`'s design note already
warns about for landing manifests. Only an externally governed
`TrustedReferenceAcquisitionBinding`, sourced outside the receipt itself, can
anchor verification.

## Trusted acquisition-join evidence boundary

`src/india_swing/reference_data/acquisition_join.py` implements
`ReferenceAcquisitionJoinService`, which bridges one already-verified
`VerifiedReferenceAcquisitionReceipt` to the exact GCS-pinned raw bytes it
names and an independently reparsed security master, producing one immutable
`VerifiedReferenceAcquisitionJoin`. It is an evidence-assembly boundary only.

`join(receipt)` first independently re-verifies the receipt
(`receipt.verify_content_identity()`) before any read; a mutated or invalid
receipt causes zero reads. It then calls the caller-injected, exact
`GCSLandingObjectReader` exactly once, pinned to `receipt.landing_object`
(never a listing or "latest" read). It never trusts the returned
`AcquiredFile` merely because the reader constructed it: it independently
recomputes the raw SHA-256 over the acquired bytes and requires it to agree
with both the acquired file's own claimed hash and the receipt's own
`raw_sha256`; requires bucket/object name/generation/session/file-type to
agree with `receipt.landing_object`; derives the parser's `original_filename`
only from the canonical final path component of
`receipt.landing_object.object_name` (never a separately supplied filename)
and requires it to match the filename implied by `receipt.report_date`; and
freshly reparses the retained bytes with a new `NseCmSecurityMasterParser`,
rejecting any result whose `excluded_alternative_venue_count` is nonzero
(this receipt schema authorizes only the NSE Listed securities report, never
the NSE Listed and BSE Exclusive interoperability file). The resulting
`join_id` is a deterministic, full lowercase SHA-256 `content_id` over a
complete canonical mapping of every lineage component: schema, receipt hash,
raw hash and byte count, GCS bucket/object/generation, target report date,
parser/source/scope semantics, header/uncompressed/ordered-row hashes, and
every row/disposition count.

`VerifiedReferenceAcquisitionJoin.__post_init__` calls
`verify_content_identity()`, which independently replays the receipt's own
defensive check plus the same join-derivation routine `join()` uses, and
requires exact type-and-value agreement with every retained field — so
direct construction with a mismatched typed value, or post-construction
`object.__setattr__` mutation of any field (including inside the nested
receipt, its binding, its landing object, the acquired file, or a parsed
record), fails closed with one static sanitized `ReferenceAcquisitionJoinError`.

**This evidence is still not promotion, readiness, or trading authority.**
`VerifiedReferenceAcquisitionJoin` carries no `AcquisitionMode`, readiness,
`actionable`, `verified_report_date`, promotion, signal, recommendation,
notification, order, broker, or capital field. `ReferenceAcquisitionJoinService`
performs no bucket listing, "latest" selection, filesystem read/write,
environment access, wall clock, network/GCS client construction, retry loop,
or implicit fallback anywhere in the module; production I/O is reachable
only through the injected, already-constructed `GCSLandingObjectReader`. A
separate, subsequent decision must still explicitly promote this joined
evidence into any `AcquisitionMode`/readiness/`verified_report_date` change
on a stored reference artifact.

## Trusted artifact-promotion boundary

`src/india_swing/reference_data/acquisition_promotion.py` implements
`ReferenceArtifactPromotionService`, which proves that one existing sealed
`COLLECTION_ONLY` security-master artifact -- produced by the manual
importer and unchanged since -- is exactly the artifact represented by one
already-verified `VerifiedReferenceAcquisitionJoin`, then emits a separate
immutable `VerifiedReferenceArtifactPromotion` evidence record. It never
rewrites, replaces, renames, or otherwise mutates the source archive: the
original sealed manifest, raw bytes, and normalized bytes remain byte-for-
byte unchanged, and no second archive or promotion store is created.

`promote(join, artifact)` first requires exact
`VerifiedReferenceAcquisitionJoin` and `StoredReferenceArtifact` types,
verifies the join's own content identity, and calls the artifact store's
`verify_stored_reference_provenance` before deriving any promoted fact --
so a synthetic, shaped, or already-mutated artifact fails closed before any
comparison runs. It then requires the source manifest to remain in exactly
its original collection-only state (`UNVERIFIED_MANUAL_FILE`,
`COLLECTION_ONLY`, `actionable=false`, `verified_report_date=None`,
`publication_time_status="UNVERIFIED_MANUAL_FILE"`), independently proves
byte-for-byte and semantic-fact-for-semantic-fact equality between the
trusted join and the sealed artifact (raw bytes/hash/byte count, the
complete parsed security master, report date, filename, source URL, media
type, parser/source/scope/codec versions, every hash, and every
row/disposition count), and recomputes `encode_security_master(join.parsed)`
and its SHA-256 rather than trusting the retained normalized bytes or
`manifest.normalized_sha256`.

The resulting `promotion_id` is a deterministic, full lowercase SHA-256
`content_id` over a canonical mapping of the join, receipt, source
artifact/manifest identity, every trusted hash, the report date, the
knowledge time, and the fixed promoted acquisition mode/readiness/
actionable facts -- no filesystem path, mtime, repr, or runtime identity, so
identical content stored under a different path yields the same
`promotion_id`.

`VerifiedReferenceArtifactPromotion` retains the exact join and source
artifact plus only the derived promotion facts: `schema_version`,
`promotion_id`, `promoted_acquisition_mode=TRUSTED_PINNED_GCS_RECEIPT`,
`promoted_readiness=POINT_IN_TIME_VERIFIED`, `verified_report_date` (from
the receipt), `knowledge_time` (the receipt's own `acquired_at`, never a
wall clock, filesystem timestamp, or first-seen time), and
`actionable=false`. `__post_init__` calls `verify_content_identity()`, which
independently replays every one of these checks, so direct construction
with a mismatched value or post-construction `object.__setattr__` mutation
anywhere in the retained graph fails closed.

**`POINT_IN_TIME_VERIFIED` on this record still is not promotion into any
trading, alert, or capital capability.** It means only that this exact
security-master artifact vintage has independently pinned acquisition
provenance. It does not establish stable cross-vintage identity, calendar,
universe, surveillance, liquidity, prices, corporate actions, model
validation, alert eligibility, or profitability, is not a
`PromotionEvidence`/`PromotionDecision` record, and does not itself change
the stored artifact's own `AcquisitionMode`, readiness, or `actionable`
flag on the sealed archive.

## Durable promotion-replay store

`src/india_swing/reference_data/promotion_store.py` implements
`LocalReferenceArtifactPromotionStore`, the durable root store that lets
`VerifiedReferenceArtifactPromotion` survive a process restart. It is a
persistence boundary only: the stored manifest retains the promotion/join
IDs, the exact source artifact/manifest IDs and raw/normalized hashes, the
exact receipt bytes, and every field of the exact
`TrustedReferenceAcquisitionBinding` -- but none of that is trusted as
authority on its own. The persisted binding is a local trust-root input, not
something independently authenticated by a self-consistent manifest: a
manifest that agrees with itself only proves the file was not corrupted, not
that the trust boundary it records is the one that was originally accepted.
`get(promotion_id)` always resolves only the exact pinned
`StoredReferenceArtifact` through the caller-supplied
`LocalReferenceArtifactStore`, reconstructs the trusted binding and verified
receipt from the retained fields, joins through a private in-process reader
that serves only the sealed artifact's own already-verified raw bytes for the
exact receipt-pinned bucket/object/generation (never GCP, network, or a
bucket listing), and re-promotes -- requiring exact agreement with the stored
`promotion_id` before returning anything. The upstream promotion identity
(`reference-artifact-promotion/v2`) content-binds every field of the trusted
binding -- `expected_receipt_sha256`, `expected_raw_sha256`, `allowed_bucket`,
`target_report_date`, `not_before`, `cutoff`, and `trusted_acquirer_id` --
into the promotion's own `promotion_id`. This means an in-place change to any
one binding field, even one that is still individually valid (for example
widening `cutoff` by a day), cannot silently redefine the trust boundary
under the original `promotion_id`: the independently reconstructed
`promotion_id` differs from the pinned one and `get` fails closed instead of
accepting the widened binding. `put(promotion)` performs the identical replay
before writing, so a promotion whose retained receipt/binding cannot be
independently reconstructed from the pinned sealed artifact leaves no target
file behind. Exposes only `put`, `get`, and `path_for`; there is no list,
latest, or nearest-selection capability. A successfully stored promotion is
still not trading, alert, or capital authority, and it does not change the
sealed reference artifact's own `AcquisitionMode`, readiness, or `actionable`
flag.

## Calendar source hierarchy

The human-facing source is NSE's
[market timings and holidays page](https://www.nseindia.com/resources/exchange-communication-holidays).
Its holiday JSON is an undocumented page backend, not a complete schedule.
Rows can include “Special Live Trading,” and a listed holiday can later receive
Muhurat timings. Calendar materialization must therefore be event-sourced:

1. versioned regular CM schedule;
2. annual holiday circular;
3. later closure/amendment circulars;
4. explicit special-session circulars with exact windows;
5. no mock/contingency session treated as live trading.

Examples of why overrides matter:

- [CMTR71775](https://nsearchives.nseindia.com/content/circulars/CMTR71775.pdf)
  published the 2026 base holidays while leaving Muhurat timings pending.
- [CMTR72260](https://nsearchives.nseindia.com/content/circulars/CMTR72260.pdf)
  added an ad-hoc January 2026 closure only days beforehand.
- [CMTR70319](https://nsearchives.nseindia.com/content/circulars/CMTR70319.pdf)
  defined nonstandard 2025 Muhurat timings.
- [MSD61893](https://nsearchives.nseindia.com/content/circulars/MSD61893.pdf)
  defined a Saturday live DR session with a nonstandard close.

The code requires one explicit `CalendarDay` for every covered date. Missing
dates, unknown special-session times, post-cutoff source vintages, or coverage
that does not reach the trade horizon fail closed.

## Implemented contract

The current reference contracts supply:

- external records with event time, knowledge time, source snapshot, and hash;
- eligibility records with separate half-open effective-session intervals,
  bound to the exact instrument/listing and supported state values;
- validity-dated listing mappings keyed by an opaque audited instrument ID and
  bound to an exchange and segment;
- explicit unknown states and one disposition for every scoped master row;
- collection-only, point-in-time-verified, and synthetic-test readiness states;
- complete calendar coverage with regular, special, holiday, weekend, and
  unscheduled-closure dates, including multiple real windows on split sessions;
- typed session-window phases: only `LIVE_CONTINUOUS` is executable, while
  pre-open, call-auction, and mock-test windows remain non-executable evidence;
- content-derived calendar/universe IDs;
- exact audited dataclass types throughout the reference graph, preventing a
  subclass from overriding session executability or effective-state resolution;
- pipeline checks that bind the decision, instruments, provider outputs,
  listing mappings, universe, calendar, session, and cutoff;
- full data- and instrument-content fingerprints on forecast, signal, setup,
  and research outputs, with exact provider-version checks;
- consumption-time identity verification and final pre-decision revalidation,
  so a provider cannot mutate a validated reference, candidate, component
  configuration, or risk policy mid-run;
- exact-one effective-state resolution for the signal and proposed entry
  sessions, including adjacent REG1-style half-open rollovers; overlapping or
  missing states fail closed, and a new suspension/surveillance state blocks
  entry even when the stock was eligible on the signal day;
- a next-session gate that requires listing validity to persist through entry
  and keeps entry/expiry inside one executable live-continuous window.

`POINT_IN_TIME_VERIFIED` construction remains deliberately disabled. The MII
importer now binds original archived bytes, the approved dataset kind, dated
filename, locally observed availability, parser/schema versions, row counts,
ordered row digest, and source hashes. It cannot establish authoritative
publication time, stable cross-vintage identity, REG1 surveillance, the trading
calendar, or liquidity completeness. Merely importing this file—or wrapping
Kite, bhavcopy, or hand-built rows in reference models—cannot enable real alerts.

Synthetic decisions carry `execution_eligible=false` inside the decision itself
and identify their reference readiness. The audit writer accepts only intact
typed pipeline results for pipeline-shaped records, binds the filename to the
run ID, rejects secret-bearing fields, and detects nested mutation after finish.
Even a result-only typed audit carries the trial, model bundle, data content,
source revision, execution-policy, and cost-schedule lineage fields.
These controls do not turn synthetic data into a real trading signal.

The current next-session persistence check is intentionally strict. Before live
use, the REG1 importer must materialize the entry-session state produced after
session D and a pre-alert/pre-entry revalidation must detect later exchange
changes. The code must not rewrite a report's source event or knowledge time to
make it appear effective earlier.

Synthetic tests exercise these contracts; they do not prove official historical
coverage. Current free dated masters appear defensible only from February 2024,
and consolidated REG1 coverage starts in December 2024. A longer reportable
backtest needs NSE's licensed historical Masters data or must be labelled
unsupported for the earlier period.

## Access and licensing

Do not add an NSE website scraper. NSE's
[Terms of Use](https://www.nseindia.com/static/nse-terms-of-use) restrict
systematic/automated collection, and its
[data policy](https://www.nseindia.com/static/market-data/nse-data-policy)
addresses automated/non-display usage for trading decisions. Public download
availability is not an automation licence.

The initial ingestion path should accept manually downloaded official artifacts
or an authorized/licensed feed, then archive the original bytes with retrieval
time, URL, media type, HTTP metadata, parser version, and SHA-256 before parsing.
Recurring automated acquisition requires an approved NSE channel or written
permission from NSE Data & Analytics.
