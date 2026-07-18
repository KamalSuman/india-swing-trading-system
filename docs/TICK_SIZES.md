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
