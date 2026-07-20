# Deployment Handover: GCP Serverless Infrastructure

This document outlines the current active deployment state of the **India Equity Swing-Trading System** on Google Cloud Platform (GCP). It is designed to allow subsequent AI agents or developers to quickly understand the infrastructure topology and start working immediately.

---

## 🌍 Deployed Environment Details

* **GCP Project ID:** `indian-swing-trading-bot`
* **Target Region:** `asia-south1` (Mumbai)
* **Firestore Database:** Native Mode `(default)`
* **Cloud Storage Bucket:** `gs://swing-data-indian-swing-trading-bot` (Versioning: Enabled, Access: Uniform)
* **Artifact Registry Repository:** `swing-repo` (Path: `asia-south1-docker.pkg.dev/indian-swing-trading-bot/swing-repo`)
* **Production Docker Image:** `asia-south1-docker.pkg.dev/indian-swing-trading-bot/swing-repo/india-swing-app:latest`

---

## 🏗️ Serverless Components Status

### 1. `rss-collector` (Cloud Run Service)
* **Current State:** **Disabled.** The `gcloud run deploy` step for this service (and its dependent `rss-collector-schedule` Cloud Scheduler job) is commented out in `deploy.sh`. It is not deployed and has no service URL.
* **Why:** The RSS collector application logic does not exist yet (no `rss_collector` module, no real entrypoint such as gunicorn/uvicorn). The previous version of this script shipped a `python -m http.server` placeholder as the container command to pass Cloud Run's health checks — this was removed because running a bare file server, even behind Cloud Run authentication, is not acceptable.
* **Planned Configuration (once re-enabled):** 0-1 Instances, 1 CPU, 512 MiB RAM, 120s Timeout, Authenticated Ingress.
* **Service Account:** `rss-collector-runtime@indian-swing-trading-bot.iam.gserviceaccount.com` (created by `deploy.sh`, but currently unused since the service isn't deployed).

### 2. `eod-swing` (Cloud Run Job)
* **Job Name:** `eod-swing`
* **Configuration:** 1 Task, 1 Retry, 2 CPU, 4 GiB RAM, 90-minute timeout.
* **Service Account:** `eod-swing-runtime@indian-swing-trading-bot.iam.gserviceaccount.com`
* **Trigger status: manual only.** There is no code path in `deploy.sh` that
  creates, updates, resumes, or leaves active any automated schedule for this
  job (see Section 3 below). Run it explicitly with
  `gcloud run jobs execute eod-swing --region=asia-south1` for a bounded
  manual validation, or by setting `PINNED_GCS_RUN_SPEC_SECRET_VERSION`
  before a fresh `deploy.sh` run and then executing the job.
* **Run-spec authority: exactly one pinned Secret Manager version.** The
  job's entrypoint (`--command=python --args=-m,india_swing.cloud_job,
  --spec-file,/var/run/india-swing-control/pinned-run-spec.json`) reads its
  operator-authored pinned run-spec from a single file mounted at the fixed
  path `/var/run/india-swing-control/pinned-run-spec.json`, sourced from the
  Secret Manager secret `PINNED_GCS_RUN_SPEC` at the exact numeric version
  the operator supplies via `PINNED_GCS_RUN_SPEC_SECRET_VERSION` when running
  `deploy.sh`. `deploy.sh` fails closed before any image build or job
  mutation if that version variable is missing, empty, `latest`, zero,
  signed, whitespace-bearing, non-canonical, or if the named secret/version
  does not already exist and is not `ENABLED`. `deploy.sh` never creates,
  seeds, or hashes this secret's contents.
* **Mounted Secrets:** exactly one — `PINNED_GCS_RUN_SPEC` (pinned version)
  mounted as the file above. `KITE_API_KEY`, `KITE_API_SECRET`,
  `LLM_API_KEY`, and `NOTIFICATION_TOKEN` remain provisioned (see Secret
  Manager section below) for possible future, separately reviewed features,
  but this job's runtime service account is **not** granted access to them
  and none of them are mounted here — `deploy.sh` also revokes any
  secret-level accessor bindings left by its older deployment definition.
  `cloud_job.py` imports and calls none of those capabilities.
* **Local artifact roots (ephemeral):** the job's `--set-env-vars` points
  every `INDIA_SWING_*_ROOT` variable the pinned CLI path depends on at a
  distinct path under `/tmp/india-swing/` inside the job container
  (`calendar_data`, `identity_registry`, `historical_prices`,
  `daily_reports`, `reference_data`, `daily_pipeline`). Cloud Run Jobs do not
  persist local filesystem state between task attempts or runs, so these
  roots are suitable only for a bounded manual validation run, not for
  durable storage. **Next blocker:** real cloud artifact-store persistence
  (e.g. GCS- or Firestore-backed stores in place of the local filesystem
  stores) has not been implemented; until it is, each run starts from empty
  local stores.

### 3. Cloud Scheduler (Crons)
No cron is active for either component:
* **`eod-swing-schedule`**: No automated-activation path exists in
  `deploy.sh`. Every `deploy.sh` run pauses this schedule if it already
  exists (from a prior version of this script) and otherwise reports it
  disabled. Setting `ENABLE_EOD_SCHEDULER=true` does **not** activate
  anything — `deploy.sh` fails closed with a sanitized error before any GCP
  mutation is reachable, because a single static pinned run-spec targets one
  exact market session and previous-run binding and cannot be safely
  replayed unattended on a recurring schedule.
* **`rss-collector-schedule`**: **Disabled**, alongside the `rss-collector`
  service in Section 1 above. The scheduler job block is commented out in
  `deploy.sh` since it depends on `SERVICE_URL` from the disabled service
  deployment.

---

## 🔒 Secret Manager Keys Setup
The following Secret Manager secrets are provisioned by `deploy.sh`:
* `KITE_API_KEY`
* `KITE_API_SECRET`
* `LLM_API_KEY`
* `NOTIFICATION_TOKEN`

**Current State:** To satisfy Cloud Run’s mounting requirements during bootstrapping, each secret has been seeded with an initial **`PLACEHOLDER`** value (Version `1`). None of these four secrets are granted to, or mounted into, the `eod-swing` job (see Section 2 above) — they remain provisioned only for possible future, separately reviewed features.
**To populate real keys**, run the following in your local terminal:
```bash
echo -n "your-real-key" | gcloud secrets versions add KITE_API_KEY --data-file=-
```

A fifth secret, **`PINNED_GCS_RUN_SPEC`**, is the `eod-swing` job's sole
run-spec authority. Unlike the four above, `deploy.sh` never creates or
seeds this secret — it must already exist with an `ENABLED` version created
out of band by the operator before running `deploy.sh` with
`PINNED_GCS_RUN_SPEC_SECRET_VERSION` set to that exact version number.

---

## 💻 Developer & Agent Instructions

### How to Implement the RSS Collector Logic
The `rss-collector` deployment step is currently disabled (see Section 1 above). To bring it online:
1. Write your HTTP web server (using e.g., FastAPI, Flask, or standard library handlers) inside the Python application, with a real entrypoint (e.g., gunicorn/uvicorn) — do not fall back to `python -m http.server`.
2. In [deploy.sh](file:///C:/project/india-swing-trading-system/deploy.sh), uncomment Section 8 (`Deploy 'rss-collector' Cloud Run Service`) and replace the `--command`/`--args` placeholders with the real entrypoint.
3. Uncomment the RSS Collector Schedule block in Section 10.A, which depends on `SERVICE_URL` set in Section 8.
4. Redeploy with `deploy.sh`.

### How to Manually Trigger the EOD Analysis Job
This is a manual-trigger-only job (see Section 2 above) — there is no
automated schedule to wait for. The job's operator-authored pinned run-spec
must already be mounted from the `PINNED_GCS_RUN_SPEC` secret version
supplied when `deploy.sh` was last run. To execute a bounded manual
validation run at any time:
```bash
gcloud run jobs execute eod-swing --region=asia-south1
```
Every local artifact root the run writes to is ephemeral (`/tmp/india-swing/...`
inside the container — see Section 2) until real cloud artifact-store
persistence is implemented.

### Local Development / Testing
To test the container locally on your workstation, run:
```bash
# Build the container image locally
docker build -t india-swing-app .

# Run the CLI demo module
docker run --rm -v ${PWD}/var/audit:/app/var/audit india-swing-app
```

---

## 📈 Security & Permissions Layout
To preserve the Principle of Least Privilege:
* **`rss-collector-runtime`** service account only has write privileges to Firestore and GCS bucket object structures. It has no access to Secret Manager or AI tools. (Currently unused — see Section 1.)
* **`eod-swing-runtime`** has read/write privileges to GCS and Firestore, full `aiplatform.user` access for Vertex AI, and is granted `roles/secretmanager.secretAccessor` on exactly one secret — `PINNED_GCS_RUN_SPEC` — and not on `KITE_API_KEY`, `KITE_API_SECRET`, `LLM_API_KEY`, or `NOTIFICATION_TOKEN`.
* **Cloud Scheduler accounts** (`rss-scheduler-invoker`, `eod-scheduler-invoker`) only hold `roles/run.invoker` permissions on their respective Cloud Run targets. `eod-scheduler-invoker` currently has no active schedule to invoke (see Section 3 above).
* **Compute Engine and Cloud Build default service accounts** are granted `roles/storage.admin` scoped to the `swing-data-indian-swing-trading-bot` bucket only (via `gcloud storage buckets add-iam-policy-binding`), and `roles/artifactregistry.writer` scoped to the `swing-repo` repository only (via `gcloud artifacts repositories add-iam-policy-binding`) — **not** project-wide. This was tightened from an earlier version of `deploy.sh` that granted both roles at the project level, which would have given these default service accounts admin access to every bucket and write access to every Artifact Registry repository in the project.
