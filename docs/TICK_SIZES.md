# NSE CM tick-size evidence

Status: collection-only materialization, exact-source replay storage, promotion
evidence, and a sanitized CLI are implemented.

The NSE CM MII security master exposes `BidIntrvl` as integer paise. The
materializer converts it to rupees only through exact decimal division by 100.
It does not use the reserved ISO-extension `TickSz` field. If that field becomes
populated, materialization stops until the changed source contract is reviewed.

Each retained equity row produces one content-addressed observation containing
the claimed market session, local knowledge time, source artifact/manifest/row
IDs, session-scoped financial instrument ID, symbol, series, validated ISIN when
available, and bid interval in paise. Non-equity and other excluded source rows
do not enter the tick-size snapshot.

Materialize from an already sealed security master:

```powershell
python -m india_swing.tick_sizes.cli materialize `
  --security-master-id <artifact-id> `
  --cutoff <ISO-8601-time-with-offset>
```

The local store reopens and reparses the exact security-master gzip on every
read, reconstructs the snapshot, and requires exact equality. Snapshot files
are content addressed and create once. `show --snapshot-id <id>` and `list`
provide sanitized summaries.

The current snapshots remain `COLLECTION_ONLY`, `actionable=false` because the
manual source date and acquisition channel are unverified and observations are
not yet joined to promoted stable listing identities. They can be attached to a
promotion decision as explicit tick-size evidence, replacing
`MISSING_TICK_SIZES` with the more precise collection, coverage, identity, and
provenance blockers.

The next step is to resolve consecutive observations through adjudicated stable
listing mappings and construct effective intervals. No present-day tick size
may be projected backward across a historical change.

## Trusted promoted-session tick-size attachment

`src/india_swing/tick_sizes/promoted_session.py` implements
`PromotedSessionTickSizeService`, a separate bridge attaching exact
same-session tick-size observations from the selected trusted promoted
security master to every retained row of one
`VerifiedPromotedSessionMarketDataFrame`. It is a deliberate explicit
submodule API -- `tick_sizes/__init__.py` does not import it -- so the new
boundary cannot create a package cycle with `market_data`/`universe`.

`PromotedSessionTickSizeService.materialize(frame, cutoff)` independently
replays the frame's own content identity, requires it to remain
`COLLECTION_ONLY` with `actionable`/`training_eligible`/`alert_eligible`/
`execution_eligible` all `False`, finds the exact retained promotion the
frame's own universe already selected and independently re-verifies its
`promotion_id`, `verified_report_date`, artifact/manifest IDs, and raw/
normalized hashes against the universe's own retained lineage, and requires
`cutoff` at or after both the frame's and that promotion's own
`knowledge_time`. For every `RETAINED_UNVERIFIED_EQUITY` row -- resolved or
unresolved identity alike, since identity resolution never controls tick
coverage -- it requires exactly one selected master row matching source
artifact/manifest/record ID, financial instrument ID, symbol, series, and
validated ISIN; requires the reserved `TickSz` column to remain empty
(rejecting it outright, exactly like the legacy collection-only
materializer, if the source contract ever changes); and constructs one
`CollectedTickSizeObservation` from that row's own `BidIntrvl` and the
trusted promotion's own `knowledge_time`. Excluded source rows receive their
own exact `PromotedSessionTickStatus` exclusion status and no observation.
`effective_interval_verified` is always `False` on every entry: a single
same-session `BidIntrvl` value is evidence, not an effective-dated tick-size
interval, and a bar's presence or absence in the frame never adds, removes,
or changes a tick observation -- attachment is driven only by the exact
selected master row.

`VerifiedPromotedSessionTickSnapshot` retains the exact frame, the selected
promotion/artifact/manifest IDs and hashes, cutoff, `knowledge_time`, every
ordered entry, exact status/reason counts, and the fixed
`readiness=COLLECTION_ONLY`/`actionable=false`/`training_eligible=false`/
`alert_eligible=false`/`execution_eligible=false` flags. `__post_init__`
calls `verify_content_identity()`, which requires the exact concrete type
(rejecting subclasses/impostors) and independently replays the complete
derivation, so direct construction with a mismatched value or
post-construction mutation anywhere in the retained graph fails closed with
one static sanitized `PromotedSessionTickSizeError`. This boundary never
creates an effective tick-size interval, `EffectiveTickSize`, store, codec,
CLI, scheduler, promotion decision, feature panel, signal, alert,
notification, order, or broker integration; the legacy
`CollectionTickSizeSnapshot`/`materialize_collection_tick_sizes` path
remains completely unchanged.

## Trusted promoted effective-session tick-size promotion

`src/india_swing/tick_sizes/effective_session.py` implements
`PromotedEffectiveSessionTickService`, a separate explicit-submodule bridge
converting exact promoted same-session `BidIntrvl` observations for resolved
stable listings into point-in-time verified `EffectiveTickSize`
specifications (the existing `evaluation.dataset_assembly.EffectiveTickSize`
type, reused unchanged) -- each bounded to the one calendar date it was
actually observed on, never merged, extended, or left open-ended.

`PromotedEffectiveSessionTickService.materialize(source_panel, cutoff)`
independently replays the source `VerifiedPromotedStableListingHistoryPanel`'s
own content identity, requires it to remain `COLLECTION_ONLY` with every
eligibility flag `False`, and requires `cutoff` at or after the source
panel's own `knowledge_time`. It produces exactly one
`PromotedEffectiveSessionTickResult` for every `(stable_instrument_id,
stable_listing_id, market_session)` cell already present in
`source_panel.histories x source_panel.sessions`, canonically ordered by
stable IDs then session -- no resolved history or calendar-grid session can
disappear. A cell whose retained history observation still carries its
exact `PromotedSessionTickEntry` becomes `VERIFIED_EXACT_SESSION_ONLY` and
carries exactly one `EffectiveTickSize` with `effective_from_session` equal
to the observed session, `effective_to_exclusive` equal to that session plus
one calendar day, `tick_size` equal to the observation's own
`tick_size_rupees`, `source_snapshot_id`/`knowledge_time` bound to the exact
matching source tick snapshot, and `readiness=POINT_IN_TIME_VERIFIED`; the
retained `effective_interval_verified` flag on the source entry is
independently required to remain exactly `False` before promotion, and the
source entry itself is never mutated or relabeled. A cell with no tick
entry -- because its whole session snapshot or universe row is missing --
becomes `MISSING_OBSERVATION_BLOCKED` with `tick_specification=None` and
static reasons; no adjacent-session value is ever forward-filled,
backfilled, interpolated, or copied. Bar status is irrelevant to tick
verification: a retained exact tick observation still promotes whether its
price bar was observed, absent, or in identity conflict. Unresolved and
source-excluded rows remain retained only through `source_panel.
unassigned_entries` and never receive a stable-listing tick specification.

`resolved_histories_tick_coverage_complete` is `True` only when the source
panel has at least one resolved history and every produced result is
`VERIFIED_EXACT_SESSION_ONLY`; it says nothing about whole-universe identity
or tick coverage. `VerifiedPromotedEffectiveSessionTickPanel` retains the
exact source panel, cutoff, `knowledge_time`, every ordered result, exact
status/reason counts, that completeness flag, and the fixed
`readiness=COLLECTION_ONLY`/`actionable=false`/`training_eligible=false`/
`feature_eligible=false`/`alert_eligible=false`/`execution_eligible=false`
flags. `__post_init__` calls `verify_content_identity()`, which requires the
exact concrete type and independently replays the complete derivation, so
direct construction with a mismatched value or post-construction mutation
anywhere in the retained graph fails closed with one static sanitized
`PromotedEffectiveSessionTickError`. This boundary does not join tick
specifications to adjusted prices, and creates no feature panel, evaluation
dataset, signal, model score, ranking, alert, notification, order, position
size, broker action, store, codec, CLI, scheduler, or GCP integration; the
legacy `evaluation.dataset_assembly`/`historical_prices.promoted_history`
paths remain completely unchanged.
