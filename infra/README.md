# Low-cost GCP MVP

Status (updated 2026-07-18): a first deployment pass exists — see
`Dockerfile`, `deploy.sh`, and `DEPLOYMENT_HANDOVER.md` at the repository
root for the actual current state of the `indian-swing-trading-bot` GCP
project. This document below remains the target architecture; where it
differs from `DEPLOYMENT_HANDOVER.md`, the handover doc reflects reality.

Notably still true: the `rss-collector` service described below has **no
real application logic or entrypoint yet** — its Cloud Run deployment step
is currently disabled in `deploy.sh` rather than shipped with a placeholder
`python -m http.server` (an earlier version of the script did this; it was
removed as insufficiently secure even behind Cloud Run authentication). The
`eod-swing` Cloud Run Job and its scheduler are deployed and configured.
There is still no cloud persistence of real trading data, and no
notification adapter.

This deployment is for an end-of-day Indian equity swing-alert system. It collects official events continuously, runs one batch analysis after the market closes, and sends a recommendation for manual review. It does **not** place, modify, or cancel broker orders.

## Architecture

```text
Cloud Scheduler (every 5 minutes, Asia/Kolkata)
    -> authenticated Cloud Run service: rss-collector
        -> licensed or officially supported NSE/RBI/SEBI event sources
        -> raw source objects in Cloud Storage
        -> event metadata and deduplication keys in Firestore

Cloud Scheduler (20:15 IST, Monday-Friday)
    -> authenticated Cloud Run Job: eod-swing
        -> validate the trading date and market data
        -> screen the liquid NSE universe deterministically
        -> run Kronos on the shortlist
        -> join point-in-time official events
        -> run the adapted TradingAgents review on finalists only
        -> apply deterministic risk and event vetoes
        -> persist the report
        -> send one alert for manual execution
```

Use `asia-south1` for Cloud Run, Artifact Registry, and Cloud Storage where practical. Keeping resources together reduces latency and avoids unnecessary cross-region data transfer.

## Components

### Cloud Scheduler

Create two schedules with timezone `Asia/Kolkata`:

- RSS collection: `*/5 * * * *`. Running continuously also captures late company filings.
- EOD analysis: `15 20 * * 1-5`. The job must still consult the configured NSE trading calendar and exit successfully on holidays.

Scheduler is only the clock. It should invoke authenticated targets with a dedicated service account. Use OIDC for the Cloud Run service and OAuth when calling the Cloud Run Jobs API.

### `rss-collector` Cloud Run service

This is a short HTTP request workload, so a scale-to-zero service is cheaper and simpler than starting a batch job every five minutes.

Suggested initial configuration:

- minimum instances: `0`;
- maximum instances: `1`;
- CPU: `1`;
- memory: `512 MiB`;
- request-based billing;
- authenticated ingress only;
- request timeout: 60-120 seconds.

For each source item, retain its source publication timestamp, first-seen timestamp, canonical URL, source/category, symbol or issuer identifiers, revision status, and content hash. Save the raw response and linked document only where the provider's terms permit it. Do not scrape undocumented NSE or BSE website endpoints. The exact sources and licences must be selected before this collector is implemented.

The collector should also emit a heartbeat. A stale heartbeat is a reason to suppress the EOD recommendation or mark it explicitly as incomplete.

### `eod-swing` Cloud Run Job

The analysis is a finite batch process, so it belongs in a Cloud Run Job rather than a long HTTP request.

Suggested initial configuration:

- tasks: `1`;
- retries: `1`;
- CPU: `2`;
- memory: `4 GiB`, increased only after profiling;
- task timeout: 60-90 minutes;
- no GPU for the MVP.

The job should narrow the universe before using expensive models. A sensible sequence is deterministic liquidity and quality filters, deterministic strategies/features, Kronos on roughly 20-40 candidates, then the TradingAgents LLM review on only 3-8 finalists. This prevents LLM cost from scaling with the entire exchange.

The final selection and position sizing remain deterministic. A model response cannot bypass liquidity, corporate-action, stale-data, event-risk, exposure, or maximum-loss rules.

## Persistence

Cloud Run container filesystems are disposable. Do not rely on a local SQLite database, downloaded files, or model cache surviving another execution.

### Cloud Storage

Use one regional bucket with uniform bucket-level access and object versioning where appropriate. Suggested prefixes are:

```text
raw/events/<source>/<date>/...
market/eod/<date>/...
features/<feature-version>/<date>/...
reports/<trade-date>/<run-id>.json
models/kronos/<model-version>/...
```

Store tabular market data and features as Parquet. Store raw feed items and final reports as immutable objects. Apply lifecycle rules only after deciding how much history is required for point-in-time backtests.

For the small public Kronos checkpoint, the simplest reproducible deployment is to pin the model and tokenizer versions and bake them into the container image. If weights are kept in Cloud Storage instead, download a versioned object to the job's temporary filesystem, verify its SHA-256 digest, and load it locally. Model weights are artifacts, not secrets.

Do not put SQLite on a Cloud Storage FUSE mount. Cloud Storage is object storage and does not provide the locking and full POSIX semantics SQLite expects.

### Firestore

Use the default Firestore database for small mutable operational records:

- `events/{event_hash}` for deduplication and first-seen metadata;
- `runs/{trade_date_model_version}` for the execution lock and stage status;
- `signals/{signal_id}` for immutable decision metadata;
- `notifications/{signal_id_channel}` for delivery idempotency;
- `active_recommendations/{symbol}` for late-event invalidation checks.

Large raw documents, candles, prompts, and reports belong in Cloud Storage, not Firestore.

### Secret Manager

Store only credentials and sensitive configuration in Secret Manager, including market-data credentials, hosted-LLM API keys, and notification tokens. Grant the runtime service account `Secret Manager Secret Accessor` on the individual secrets it needs, not at project scope.

Prefer workload identity and Application Default Credentials for Google Cloud APIs. If TradingAgents uses Vertex AI, authorize the runtime service account directly instead of creating a Google API key. Pin secret versions when exposed as environment variables; a mounted secret file is more convenient when automatic rotation is required.

Never include secrets in container images, normal environment-variable files, source control, Firestore documents, reports, prompts, or logs.

## Idempotency and failure handling

Scheduler invocations and Cloud Run task retries can cause the same trading date to be processed more than once. Every external effect must therefore be idempotent.

At job startup, create or transactionally acquire `runs/{trade_date_model_version}` with a lease. Then:

1. Exit if the same version already completed successfully.
2. Exit if another execution holds an unexpired lease.
3. Recover from a stale lease using persisted stage checkpoints.
4. Write stage outputs to versioned paths before marking the stage complete.
5. Create a deterministic `signal_id` from trade date, symbol, strategy version, model version, and decision type.
6. Send a notification only if `notifications/{signal_id_channel}` does not already record successful delivery.

Treat revisions to exchange filings as new events; never overwrite the point-in-time record. An event is eligible only after its source dissemination timestamp. An after-close event must not appear in an earlier historical decision.

If market data, corporate-action adjustment, the official-feed heartbeat, model inference, or risk validation fails, persist the failure and send an operational error. Do not fall back to a lower-quality trade alert silently.

Use structured Cloud Logging entries containing the run ID, stage, data date, model versions, duration, candidate count, and status. Do not log raw secrets, full prompts, or large source documents. Add an alert for failed EOD executions and missing collector heartbeats.

## IAM

Use separate service accounts:

- `rss-scheduler-invoker`: can invoke only the collector service;
- `eod-scheduler-invoker`: can execute only the EOD job;
- `rss-collector-runtime`: can write the event prefixes and event metadata;
- `eod-swing-runtime`: can read input data, write reports and run state, access only required secrets, and optionally call Vertex AI;
- deployment identity: can build/deploy resources but is not used at runtime.

Avoid broad `Owner`, `Editor`, or project-wide secret-access roles. No runtime identity needs broker order permissions because the MVP is alerts-only.

## No automatic execution

The system's terminal output is a notification containing the proposed symbol, direction, entry condition or range, stop/invalidation, holding horizon, maximum position size, confidence/quality flags, and supporting source links. The user makes the final decision and manually enters any order.

Do not deploy an order-writing endpoint, store order-placement permissions, or allow the LLM to invoke broker trade tools. The Zerodha MCP available in an interactive Codex session is not automatically a production data or execution interface for Cloud Run; any production market-data access must use a supported API and separately managed credentials.

## Cost controls

- Keep both Cloud Run targets at zero when idle.
- Use one EOD task before introducing parallel tasks.
- Run Kronos only on the deterministic shortlist and TradingAgents only on finalists.
- Set hard per-run limits for LLM calls, tokens, retries, and candidates.
- Co-locate compute and storage; do not add a Serverless VPC connector unless a private resource requires it.
- Avoid Cloud SQL, an always-on VM, GKE, and a GPU for this stage.
- Configure a small billing budget and alerts. A budget warns about spend but does not automatically cap it.

Cloud billing and free-tier rules change. Estimate them against the selected
region and current pricing before deployment. Hosted LLM calls and licensed
market data are still likely to dominate this small batch workload.

## Phase 2: Workflows

Add Workflows when the monolithic EOD job becomes difficult to retry or observe. A future workflow can coordinate separate jobs:

```text
validate-data
    -> build-features-and-screen
    -> run-kronos
    -> event-and-agent-review
    -> deterministic-risk-decision
    -> publish-alert
```

Pass only a `run_id`, trading date, configuration version, and Cloud Storage object paths between steps. Do not pass candle arrays or documents through workflow state. Each job should keep the same Firestore checkpoints and deterministic output paths so an individual stage can retry without repeating successful LLM calls or sending duplicate notifications.

Workflows adds useful per-stage retries, branching, auditability, and failure handling. It is not a compute runtime, database, or substitute for application-level idempotency. Keep the direct Scheduler-to-Job design until these operational benefits justify the additional deployment surface.
