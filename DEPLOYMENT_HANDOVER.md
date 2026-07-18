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
* **Mounted Secrets:**
  * `KITE_API_KEY` -> Environment variable `KITE_API_KEY:latest`
  * `KITE_API_SECRET` -> Environment variable `KITE_API_SECRET:latest`
  * `LLM_API_KEY` -> Environment variable `LLM_API_KEY:latest`
  * `NOTIFICATION_TOKEN` -> Environment variable `NOTIFICATION_TOKEN:latest`

### 3. Cloud Scheduler (Crons)
Only one cron is active, in the `Asia/Kolkata` timezone:
* **`eod-swing-schedule`** (`15 20 * * 1-5`): Triggers the `eod-swing` batch job every weekday (Mon-Fri) at 8:15 PM India Time. Authenticates securely using an OAuth token bound to the `eod-scheduler-invoker` service account.
* **`rss-collector-schedule`**: **Disabled**, alongside the `rss-collector` service in Section 1 above. The scheduler job block is commented out in `deploy.sh` since it depends on `SERVICE_URL` from the disabled service deployment.

---

## 🔒 Secret Manager Keys Setup
The following Secret Manager secrets are provisioned:
* `KITE_API_KEY`
* `KITE_API_SECRET`
* `LLM_API_KEY`
* `NOTIFICATION_TOKEN`

**Current State:** To satisfy Cloud Run’s mounting requirements during bootstrapping, each secret has been seeded with an initial **`PLACEHOLDER`** value (Version `1`). 
**To populate real keys**, run the following in your local terminal:
```bash
echo -n "your-real-key" | gcloud secrets versions add KITE_API_KEY --data-file=-
```

---

## 💻 Developer & Agent Instructions

### How to Implement the RSS Collector Logic
The `rss-collector` deployment step is currently disabled (see Section 1 above). To bring it online:
1. Write your HTTP web server (using e.g., FastAPI, Flask, or standard library handlers) inside the Python application, with a real entrypoint (e.g., gunicorn/uvicorn) — do not fall back to `python -m http.server`.
2. In [deploy.sh](file:///C:/project/india-swing-trading-system/deploy.sh), uncomment Section 8 (`Deploy 'rss-collector' Cloud Run Service`) and replace the `--command`/`--args` placeholders with the real entrypoint.
3. Uncomment the RSS Collector Schedule block in Section 10.A, which depends on `SERVICE_URL` set in Section 8.
4. Redeploy with `deploy.sh`.

### How to Manually Trigger the EOD Analysis Job
To test the stock scanning engine and strategy models manually at any time, run:
```bash
gcloud run jobs execute eod-swing --region=asia-south1
```

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
* **`eod-swing-runtime`** has read/write privileges to GCS and Firestore, full `aiplatform.user` access for Vertex AI, and is the only entity granted `roles/secretmanager.secretAccessor` on the 4 production secrets.
* **Cloud Scheduler accounts** (`rss-scheduler-invoker`, `eod-scheduler-invoker`) only hold `roles/run.invoker` permissions on their respective Cloud Run targets.
* **Compute Engine and Cloud Build default service accounts** are granted `roles/storage.admin` scoped to the `swing-data-indian-swing-trading-bot` bucket only (via `gcloud storage buckets add-iam-policy-binding`), and `roles/artifactregistry.writer` scoped to the `swing-repo` repository only (via `gcloud artifacts repositories add-iam-policy-binding`) — **not** project-wide. This was tightened from an earlier version of `deploy.sh` that granted both roles at the project level, which would have given these default service accounts admin access to every bucket and write access to every Artifact Registry repository in the project.
