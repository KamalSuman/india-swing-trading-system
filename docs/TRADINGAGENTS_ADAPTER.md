# TradingAgents Adapter Contract

Status: design contract
Upstream project: [TauricResearch/TradingAgents](https://github.com/TauricResearch/TradingAgents)
Pinned baseline: `v0.3.1` at commit `01477f9afb7a47b849ed4c9259d3a9a4738d9fda`

## Purpose

TradingAgents is an advisory research stage in the India swing pipeline. It reviews a small set of candidates already selected by the deterministic NSE scanner and Kronos forecasting stage. It does not scan the market, rank the full universe, size positions, approve trades, send notifications, or communicate with a broker.

The adapter is the only application module allowed to depend on TradingAgents. All callers use the contract in this document rather than importing upstream graph, state, agent, or dataflow classes directly.

## Boundary

The surrounding application owns this sequence:

```text
Eligible NSE universe
  -> deterministic features and Kronos forecasts
  -> cross-sectional ranking
  -> small finalist set
  -> TradingAgents adapter research
  -> deterministic eligibility, cost, and risk gate
  -> one alert or NO_TRADE
```

TradingAgents provides qualitative challenge and synthesis. Its recommendation is evidence for the final gate, never authority to bypass it.

## Pinned dependency

Production must install the immutable commit above, not an unpinned branch or floating version. The application lockfile must record the resolved commit and artifact hash. An upstream upgrade is a deliberate compatibility change and follows the sync process below.

The maintained fork must remain generic. NSE, Zerodha, Kronos, Indian costs, and application-specific rules belong in `india-swing`, not in the TradingAgents fork.

## Adapter interface

The conceptual interface is:

```python
assess(request: CandidateResearchRequest) -> CandidateResearchAssessment
```

`CandidateResearchRequest` contains:

- A unique run and candidate identifier.
- Exact NSE symbol, stable instrument/listing IDs, and the instrument-content
  fingerprint used by the scanner.
- An immutable `as_of` timestamp in Asia/Kolkata.
- The point-in-time price, volume, technical, fundamental, corporate-action, announcement, sector, market-regime, and forecast context prepared by the application.
- Kronos distribution summaries and deterministic signal evidence.
- Data-quality flags and explicit unavailable fields.
- The requested swing horizon.

The request must not contain broker credentials or grant access to order APIs. Context is bounded, serializable, and persisted by the application for replay and audit.

`CandidateResearchAssessment` contains:

- Candidate and run identifiers.
- The pinned TradingAgents revision and selected model identifiers.
- Advisory rating: `Buy`, `Overweight`, `Hold`, `Underweight`, or `Sell`.
- Bull case, bear case, risk synthesis, and final research thesis.
- Any suggested levels or horizon, clearly marked as advisory.
- Data-quality warnings and missing-source disclosures.
- Execution status, failure category, elapsed time, model-call count, and token/cost metadata when available.
- References to the persisted full report and input snapshot.
- Exact universe, data-snapshot, data-content, and instrument fingerprints echoed
  from the request; any mismatch invalidates the assessment.

It does not contain an approved order, final quantity, guaranteed confidence, or permission to notify or execute. Invalid, incomplete, timed-out, or unparseable results fail closed and cannot become a trade alert.

The adapter must expose stable identity material containing only immutable
configuration: pinned source revision, graph configuration, prompts, model IDs,
and decoding parameters. Runtime clients and caches stay private and outside
that material. The pipeline fingerprints this identity before the first call
and recomputes it at finalization; a configuration change fails the run closed.

## Retained upstream components

Retain with minimal changes:

- LangGraph workflow and conditional routing.
- Bull and bear researchers.
- Research Manager, Trader, risk debaters, and Portfolio Manager as research roles.
- LLM provider clients, retry support, and structured-output helpers.
- Report rendering and state serialization utilities.
- Instrument-identity and explicit no-data protections.
- Optional per-run LangGraph checkpoint support if it proves useful for recovery.

The upstream five-tier rating remains an advisory label. The application must not treat the upstream `Portfolio Manager` as a real portfolio optimizer because it does not own account state, holdings, or the final risk policy.

## Replaced by application components

Replace these responsibilities outside the fork:

- Yahoo Finance and Alpha Vantage price history with the application's normalized NSE/Kite point-in-time store.
- Upstream technical analysis with deterministic application features and Kronos forecast summaries.
- Generic news, macro, and fundamental retrieval with timestamped, curated inputs suitable for India.
- Free-text position sizing with the deterministic sizing engine.
- Five-session return reflection with the application's prediction ledger and walk-forward evaluator.
- Upstream persistence as the system of record with application-owned durable storage.
- The interactive CLI and container entry point with the application's headless Cloud Run Job.

Custom analysts may be supplied through generic factory hooks, but they must write the report keys expected by the retained researcher and risk stages.

## Disabled upstream behavior

Disable the following in production and historical evaluation:

- Sentiment Analyst calls to current StockTwits and Reddit feeds.
- Live Polymarket data in historical runs.
- FRED data in historical runs unless a point-in-time vintage source is used.
- Uncontrolled network tools callable directly by an LLM.
- `TradingMemoryLog` as a learning or performance-measurement mechanism.
- Upstream Yahoo cache and historical replay paths.
- Automatic broker execution; TradingAgents has no execution authority.
- The stock interactive CLI path.

The adapter may expose only curated, read-only tools backed by the immutable request snapshot. A tool must return an explicit unavailable result rather than silently substituting another vendor or fabricating a value.

## Isolated per-candidate execution

Each candidate assessment runs in a fresh TradingAgents graph instance. Instances are never reused concurrently because upstream graph objects and dataflow configuration contain mutable process state.

Isolation requirements:

- One candidate and one `as_of` timestamp per graph instance.
- Separate run identifiers, checkpoint namespace, temporary directory, logs, and output paths.
- No shared upstream memory log between candidates.
- No mutation of application portfolio state from inside the graph.
- Hard wall-clock timeout, model-call budget, retry limit, and output-size limit.
- Process or job isolation for parallel candidates; do not share one graph object across threads.
- Idempotency keyed by the request snapshot hash, adapter version, TradingAgents commit, and model configuration.
- Failed or partial assessments are recorded but excluded from the final trade-selection gate.

Only the small finalist set should enter this stage. Running the full multi-agent graph over the entire NSE universe is outside the adapter contract and would create unnecessary latency, model cost, and rate-limit pressure.

## Minimal fork patch policy

The fork should add only generic extension seams that are difficult to supply by composition:

1. Dependency injection for analyst/node factories.
2. Dependency injection for read-only tool nodes or data adapters.
3. A generic external-context field in graph state and propagation.
4. A reliable headless runner that follows the complete `propagate()` lifecycle.
5. Optional final structured-schema injection.

Do not copy upstream packages into `india-swing`, monkeypatch global vendor tables at runtime, or embed India-specific prompts in the fork. Generic fixes should be proposed upstream whenever practical.

## Upstream synchronization

The fork uses `origin` for the maintained fork and `upstream` for TauricResearch. Record the baseline with an internal tag such as `upstream-v0.3.1-01477f9`.

For each upstream release considered for adoption:

1. Fetch the signed/released upstream tag and review its changelog and dependency changes.
2. Create a dedicated sync branch from the currently deployed fork revision.
3. Merge the upstream release without squashing its history.
4. Reconcile the small extension-hook commits and update `PATCHES.md` in the fork.
5. Run the unchanged upstream test suite.
6. Run adapter contract tests, graph-shape tests, structured-output tests, historical no-network tests, failure-mode tests, and a fixed candidate replay corpus.
7. Compare model calls, latency, cost, and assessment deltas against the deployed revision.
8. Deploy to staging and canary jobs before updating the production pin.

Never sync production directly from upstream `main`. Every accepted revision receives an immutable internal tag and release note. Rollback means restoring the previous TradingAgents pin; application data and request snapshots remain compatible through the adapter contract.

## License note

TradingAgents is licensed under Apache-2.0. Preserve its license, copyright notices, and any upstream `NOTICE` file, and mark modified upstream files when distributing the fork or a derived container. India-specific application modules may remain proprietary because Apache-2.0 is not a copyleft license. Data-provider terms, Zerodha/Kite terms, model licenses, and the Kronos license require separate compliance review.
