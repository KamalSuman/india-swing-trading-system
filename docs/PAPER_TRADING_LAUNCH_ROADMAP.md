# Paper-Trading Launch Roadmap

This is the durable execution order for reaching the first safe, live paper-trading session. It is intentionally separate from alpha-research ambitions so infrastructure work cannot displace the immediate paper-launch objective.

## Guardrails

- Paper only: never place or authorize a broker order.
- Initial virtual capital is configurable; the first pilot uses INR 200,000.
- Never select a latest object or silently repair missing evidence. Every input is pinned by identity, generation, hash, and knowledge time where applicable.
- Missing, stale, inconsistent, or incomplete evidence fails closed or produces an explicit no-trade result.
- Paper results are not evidence of expected real returns until a sufficiently long, leakage-controlled forward pilot exists.
- Do not enable a recurring scheduler until manual Cloud Run executions and Telegram delivery have succeeded.

## Ordered launch sequence

1. **Historical dataset verification** — COMPLETE
   - Let the Cloud Run build verify the complete NSE archive corpus.
   - Capture the successful execution name, dataset identity, object path, generation, SHA-256, coverage, row count, and exclusions.
   - Do not continue from an unverified or partial dataset.

2. **Closed-loop paper portfolio** — COMPLETE
   - Add a deterministic rollover from the prior sealed paper portfolio and exact EOD marks into the next `SwingPortfolioSnapshot`.
   - Account for cash, marked exposure, realized P&L, unrealized P&L, NAV, costs, open risk, peak NAV, and drawdown.
   - Preserve append-only position lineage and reject forks, missing active positions, identity drift, stale marks, and duplicate realization.
   - Size new trades from a conservative risk base; do not increase risk from unsealed or transient profits.
   - The pure rollover model, canonical codec, exact-ID create-once local store, mark lineage, virtual cash accounting, realized/unrealized P&L, NAV/high-water/drawdown accounting, derived portfolio artifact, predecessor-chain checks, explicit operational rollover request, terminal-last GCS publication, and pinned restore are implemented.
   - Workflow spec v2 carries the exact genesis and predecessor lineage. The daily worker seals cutoff-bound terminal EOD marks, invokes the rollover, and records the request, rollover, and exact GCS manifest pin in its terminal output. Legacy v1 artifacts remain decodable but do not gain rollover authority.

3. **Adjusted market evidence and feature bridge** — COMPLETE
   - Ingest point-in-time corporate actions, security-master/tick evidence, and the fresh daily market bundle.
   - Produce cutoff-bound adjusted prices and deterministic research features without look-ahead or survivorship leakage.
   - The cutoff-bound raw EOD mark bridge is implemented: it uses only the terminal observation from each exact outcome job, requires that observation to be traded and present in the replay lineage, and never substitutes an older close. Corporate-action adjustment and research-feature materialization are now connected end to end.
   - The verified NSE archive forward-history adapter now has a cutoff-bound
     corporate-action adjustment boundary. Research identities require explicit,
     knowledge-timed bindings to stable corporate-action identities; split and
     bonus factors apply only before their effective session; unsupported actions,
     future-known or incomplete evidence, foreign mappings, and duplicate mappings
     fail closed. Missing mappings become visible vetoes rather than disappearing
     from the cross-section. Outputs remain collection-only with no ranking, alert,
     paper-trade, notification, or execution authority.
   - The adjusted histories now join to signal-session tick-size evidence through
     a separate immutable feature-input boundary. Only the terminal bar requires
     one point-in-time verified tick specification for the same stable instrument,
     listing, and session. Historical bars explicitly retain no tick rather than
     receiving a current, previous, next, nearest, or latest value. Missing,
     ambiguous, unverified, future-known, duplicate, or foreign signal ticks are
     vetoed or rejected. The resulting candidates retain exactly 60 adjusted bars
     and remain collection-only.
   - Deterministic technical-feature materialization is implemented by reusing
     the established promoted calculation kernel. A separately versioned,
     immutable compatibility configuration resolves the forward window explicitly:
     60 bars provide 59 session-to-session return intervals, so the longest return
     is 59 while drawdown consumes all 60 bars. Tick friction uses only the exact
     signal-session tick and the tick-history window is explicitly one session,
     so no unsupported historical tick-change claim is produced. The vector retains
     multi-horizon returns, trend, ATR, volatility, breakout, drawdown, gap,
     liquidity, contraction, and tick-friction evidence. Degenerate inputs are
     vetoed without partial vectors, and outputs remain collection-only.
   - The pure operational research-graph assembler is implemented. It accepts only
     an exact raw-history window, corporate-action snapshot, and exact-session tick
     panel; derives research-to-stable identity bindings from
     retained same-session ISIN evidence; rejects future or ambiguous lineage; and
     materializes the adjustment, feature-input, and technical-feature graph under
     one content identity. Missing signal-session tick coverage remains an explicit
     veto rather than a silent cross-section shrink.
   - The durable exact-artifact boundary is also implemented. It publishes one
     compact create-or-verify GCS manifest, pins its generation and SHA-256, and on
     restore resolves only the recorded raw-history, corporate-action, and tick
     artifact IDs. The complete graph is recomputed and every derived identity is
     compared before use; there is no listing/latest selection or stored feature
     payload to trust.
   - A standalone signal-session tick artifact is now implemented for the daily
     path. It derives one-day tick specifications directly from one exact NSE MII
     security master, retains active normal-market EQ/SM listings including both
     NSE permitted-to-trade lanes, binds the source bytes/knowledge time/cutoff,
     and persists by exact content ID. The operational graph, job, manifest restore,
     and default cloud runtime accept this artifact while preserving the legacy
     promoted-panel resolver as an exact-ID fallback; neither path lists or selects
     a latest object.
   - The exact-input operational job service now reconstructs the raw 60-session
     window through the verified NSE archive dataset stream, resolves the pinned
     corporate-action snapshot and promoted tick panel, invokes the assembler, and
     seals the manifest. Its immutable receipt binds the request, graph, object
     name, generation, and SHA-256 while retaining collection-only authority.
     Cloud Run hydration/composition and the first invocation with the real IDs
     remain; the job service itself performs no discovery, clock read, notification,
     or broker action.
   - A runnable Cloud Run entry point now composes that service from explicit
     absolute hydrated roots, the exact dataset/action/tick IDs, a canonical UTC
     cutoff, and an explicit ordered 60-session tuple. It emits one canonical
     collection-only receipt envelope and a pinned GCS manifest; malformed paths or
     arguments fail before runtime construction. The published research-dataset
     manifest is now read directly from its exact GCS generation and independently
     pinned SHA-256, while its source snapshots stay in the already-mounted corpus;
     the job never lists or copies the full 4.8-million-row market-data root. The
     remaining deployment gap is hydrating the required promoted roots and binding
     the genuine corporate-action/tick IDs for the first invocation.
   - The path-free hydrated launch and one-shot wrapper now compose the existing
     promoted-input snapshot with that exact dataset pin. The wrapper hydrates into
     a fresh ephemeral directory, keeps the market corpus on its mounted volume,
     shares one GCS client across acquisition and publication, validates the inner
     collection-only envelope, and never forwards inner failure text. The remaining
     gap is operational: produce the genuine launch artifact, deploy the wrapper,
     and run it manually with the real action/tick IDs.
   - A strict NSE equity corporate-action CSV importer and manual CLI are now
     implemented. They bind the exact CSV bytes, declared coverage, acquisition
     time, and an exact NSE security master; derive stable IDs through the same
     canonical identity scheme as the promoted graph; parse split, bonus, dividend,
     rights, merger/demerger, identifier-change, and delisting purposes; account for
     non-price meeting rows; and reject unknown purposes, missing identities,
     schema drift, or future-known evidence. The acquisition time is conservatively
     used as the earliest provable knowledge time, so importing old events today
     cannot make them available to an earlier backtest. The real 23-Jul-2026 CSV
     and exact 23-Jul security master passed an ephemeral end-to-end import (20/20
     economic rows) without modifying operational state. The genuine 12-Feb through
     12-Aug CSV is now persisted as snapshot
     `0aef166c01f8a124123887dcd1094db871d6cb1c4a4525fee15564998fc9a5b4`
     with 878 economic events and zero silent drops.
   - The first genuine exact-pin Cloud Run graph completed on 14-Aug-2026 as
     execution `india-swing-forward-paper-operational-8rj79` in 9m50.3s with one
     attempt. It reconstructed the 60-session window from dataset
     `ade90738281ca444c610804aaf52577a1f4125e1b1c74e83ad058218e25542cd`,
     produced 1,405 technical feature vectors, and preserved 1,004 explicit
     blocked outcomes. Unsupported corporate-action policies veto only the
     affected security and can never flow into features.
   - Published graph ID:
     `39605f5b799211db020e8edb96f23c145acd9a5548240afb27a7632aae40eaa6`.
     Its exact manifest generation is `1786657063624792` and its independently
     recomputed SHA-256 is
     `eb8d592ef09ab6872a519f332c7d525069c6a8a141a351b54f68f955b6853faf`.
     The receipt remained `collection_only=true`, `paper_trade_eligible=false`,
     `notification_eligible=false`, and `execution_eligible=false`.

4. **First genuine promoted research run** — IN PROGRESS
   - Run the deterministic baseline/challenger on real adjusted evidence.
   - Promote only an artifact that passes the existing research, quality, quote, liquidity, allocation, and terminal gates.
   - The pure baseline/challenger boundary is implemented over the accepted
     forward-paper graph. Both arms reuse the established promoted regime and
     specialist-score kernel, retain every upstream veto, bind exact configs and
     comparisons under content identities, and grant no promotion, paper-trade,
     notification, or execution authority.
   - The durable research manifest and exact-input job boundary are implemented.
     They pin the operational graph by object name, generation, and SHA-256;
     recompute it before use; bind both configuration and arm identities; publish
     create-once through the shared state writer; and support exact replay without
     listing or latest selection.
   - The production Cloud Run entry point and pinned deployment template are
     implemented for the first one-variable experiment: default baseline versus
     a challenger changing only the high-volatility regime threshold to 0.40.
     Launch requires the expected IDs of both code-defined configurations and the
     exact accepted operational-manifest pin. A successful real 1,405-vector
     invocation remains before this step is complete.
   - First production attempt `india-swing-forward-paper-research-wvgzc` used
     reviewed commit `4289d0b` and immutable image digest
     `sha256:f4650cf5c01d63794d506d51491df2fb882dc51d2da1d59da808ae2337a064f5`.
     It loaded all 140 required archive sessions but reached the 1,800-second
     one-task, zero-retry timeout before graph restoration completed. It
     published no research artifact. The cause was redundant use of the public
     whole-graph verification constructor after exact resolver reconstruction;
     replay now uses the existing verified-input derivation path while retaining
     final end-to-end manifest comparison. A new reviewed image and one explicit
     retry remain required.

5. **INR 200,000 paper genesis and launch package** — PENDING
   - Create the empty paper portfolio and exact reconciliation evidence.
   - Assemble and seal the promoted graph, decision package, portfolio snapshot, policy, and launch secret at an exact version.

6. **Manual Cloud Run paper session** — PENDING
   - Execute the digest-pinned job manually.
   - Verify an auditable `NO_TRADE` or `PAPER_BUY` result, immutable GCS state, and Telegram delivery.
   - Repeat manually for at least one additional session and test idempotent replay/restart.

7. **Scheduled shadow pilot** — PENDING
   - Enable the daily scheduler only after the manual acceptance checks pass.
   - Run at least 10–20 market sessions, preserving every candidate, veto, fill, exit, cost, P&L, and post-trade explanation.
   - Keep the real Zerodha account read-only and separate from the virtual portfolio.

## Current checkpoint

- Infrastructure, immutable persistence, operational gates, paper-trade lifecycle, conservative outcome simulation, GCS publication, Cloud Run components, and Telegram delivery exist.
- Cloud Run execution `india-swing-nse-archive-research-dataset-d457p` completed successfully on 11-Aug-2026 with exit code 0 and no retry.
- The verified archive dataset contains 2,849 accepted sessions and 4,792,827 records. It remains deliberately `collection_only=true`, `actionable=false`, `feature_eligible=false`, and `training_eligible=false` until the adjustment/feature controls in later steps are satisfied.
- Published dataset identity: `cbb8c74ee3978cc0cd412b73251df903e2af28bd4101f30549b4833e039b8cd2`.
- Published object: `gs://swing-data-indian-swing-trading-bot/research/nse-archive-datasets/v1/cbb8c74ee3978cc0cd412b73251df903e2af28bd4101f30549b4833e039b8cd2.json`, generation `1786469190290325`, size 537,704 bytes, SHA-256 `25a8c43e07c0b6f678d8b006881cc27e5475c67e06932dd074d4a9c043ac4c83`.
- Genuine corporate-action snapshot ID: `0aef166c01f8a124123887dcd1094db871d6cb1c4a4525fee15564998fc9a5b4`.
- Genuine 31-Jul-2026 signal-session tick-panel ID: `043d0a651476b0d8e34c97a7c02d7f97c9d9176d8920fde00c1b314f5baa84fe` (2,494 active normal-market EQ/SM listings, exact source master ID `5c3b3113e7c147a5be79a725f025b53b5d16f8f0826fc9ba5d8ed0198a6ef8d7`).
- The genuine path-free 60-session operational graph has now been built and
  independently pinned. The next immediate gap is feeding that verified feature
  graph into the first genuine promoted baseline/challenger research run; no
  paper portfolio or notification authority has been granted yet.
- The largest research gap is a validated, adjusted-data alpha; Kronos/LLM research is deliberately after the deterministic paper loop works.

Update this document when a numbered step is accepted. Do not reorder or skip steps without recording the reason and the replacement evidence.
