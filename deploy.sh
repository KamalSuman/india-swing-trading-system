#!/usr/bin/env bash
# ==============================================================================
# GCP MVP Deployment Script for India Swing Trading System
# Region: asia-south1 (Mumbai)
#
# This script provisions:
# 1. Cloud Storage Bucket & Firestore (Default Database)
# 2. Service Accounts & Narrow IAM Role Bindings
# 3. Secret Manager Placeholders
# 4. Artifact Registry Docker Repository
# 5. Build and Push of Docker Image
# 6. 'rss-collector' Scale-to-Zero Cloud Run Service
# 7. 'eod-swing' Cloud Run Job
# 8. Cloud Scheduler Crons (OIDC for Service, OAuth for Job)
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# 0. Windows/Git Bash Environment Compatibility (Auto-resolve gcloud Path)
# ------------------------------------------------------------------------------
if ! command -v gcloud &>/dev/null; then
  # Resolve User Local AppData path under Git Bash
  if [ -n "${LOCALAPPDATA:-}" ]; then
    UNIX_LOCALAPPDATA=$(echo "$LOCALAPPDATA" | sed -e 's/\\/\//g' -e 's/^\([A-Za-z]\):/\/\1/')
    GCLOUD_PATH="${UNIX_LOCALAPPDATA}/Google/Cloud SDK/google-cloud-sdk/bin"
    if [ -d "$GCLOUD_PATH" ]; then
      export PATH="$GCLOUD_PATH:$PATH"
    fi
  fi
  
  # Fallback: Check hardcoded default user locations if USER is set
  if ! command -v gcloud &>/dev/null; then
    USER_GCLOUD="/c/Users/${USER:-kamal}/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin"
    if [ -d "$USER_GCLOUD" ]; then
      export PATH="$USER_GCLOUD:$PATH"
    fi
  fi

  # Fallback: Check System-wide Program Files locations
  if ! command -v gcloud &>/dev/null; then
    SYS_GCLOUD="/c/Program Files (x86)/Google/Cloud SDK/google-cloud-sdk/bin"
    if [ -d "$SYS_GCLOUD" ]; then
      export PATH="$SYS_GCLOUD:$PATH"
    fi
  fi
fi

# Auto-configure CLOUDSDK_PYTHON on Windows if not set to prevent Microsoft Store redirects
if [ -z "${CLOUDSDK_PYTHON:-}" ]; then
  # Standard User Location Bundled Python
  BUNDLED_PY="/c/Users/${USER:-kamal}/AppData/Local/Google/Cloud SDK/google-cloud-sdk/platform/bundledpython/python.exe"
  if [ -f "$BUNDLED_PY" ]; then
    export CLOUDSDK_PYTHON="$BUNDLED_PY"
  fi
fi



# ------------------------------------------------------------------------------
# 1. Configuration & Variables
# ------------------------------------------------------------------------------
# Automatically fetch Project ID if not set
PROJECT_ID="${GCP_PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo "")}"
if [ -z "${PROJECT_ID}" ]; then
  echo "Error: No GCP Project ID found. Please set GCP_PROJECT_ID or run 'gcloud config set project <id>'." >&2
  exit 1
fi

REGION="asia-south1"
REPOSITORY="swing-repo"
IMAGE_NAME="india-swing-app"
TAG="latest"

# Unique regional bucket name
BUCKET_NAME="swing-data-${PROJECT_ID}"
FIRESTORE_DATABASE="(default)"

# EOD scheduler has no automated-activation path in this script: a single
# static pinned run-spec targets one exact market session and previous run,
# so scheduling it every weekday would replay stale inputs and waste cost.
# ENABLE_EOD_SCHEDULER=true is read only to fail closed with a sanitized
# message below; it never creates, updates, resumes, or leaves active
# eod-swing-schedule. See Section 10.
ENABLE_EOD_SCHEDULER="${ENABLE_EOD_SCHEDULER:-false}"
if [ "${ENABLE_EOD_SCHEDULER}" = "true" ]; then
  echo "Error: ENABLE_EOD_SCHEDULER=true is not supported by this deployment path. A single static pinned run-spec cannot be safely scheduled for repeated automated execution; a separately reviewed dynamic per-session control plane is required before any scheduler activation exists. This script only pauses an existing eod-swing-schedule or reports it disabled." >&2
  exit 1
fi

# ------------------------------------------------------------------------------
# 1a. Pinned run-spec Secret Manager mount: fixed identity, operator-supplied
# exact version only. This control document contains independently governed
# expected hashes; this script never synthesizes its bytes, computes a hash
# from a fetched object, seeds a placeholder version, or selects "latest".
# ------------------------------------------------------------------------------
PINNED_RUN_SPEC_SECRET_NAME="PINNED_GCS_RUN_SPEC"
PINNED_RUN_SPEC_MOUNT_PATH="/var/run/india-swing-control/pinned-run-spec.json"
PINNED_GCS_RUN_SPEC_SECRET_VERSION="${PINNED_GCS_RUN_SPEC_SECRET_VERSION:-}"

# Fail closed before any image build or job mutation: the version must be
# an exact canonical positive decimal integer -- no "latest", no leading
# zero, no sign, no whitespace, not empty, not zero.
if [ -z "${PINNED_GCS_RUN_SPEC_SECRET_VERSION}" ] || ! [[ "${PINNED_GCS_RUN_SPEC_SECRET_VERSION}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: PINNED_GCS_RUN_SPEC_SECRET_VERSION must be set to an exact canonical positive decimal integer identifying an already-created, enabled Secret Manager version of '${PINNED_RUN_SPEC_SECRET_NAME}' (no 'latest', no leading zero, no sign, no whitespace, not empty, not zero)." >&2
  exit 1
fi

# Cloud Run Names
SERVICE_NAME="rss-collector"
JOB_NAME="eod-swing"

# Service Accounts
SCHEDULER_SERVICE_ACCOUNT="rss-scheduler-invoker"
JOB_SCHEDULER_SERVICE_ACCOUNT="eod-scheduler-invoker"
COLLECTOR_RUNTIME_SERVICE_ACCOUNT="rss-collector-runtime"
JOB_RUNTIME_SERVICE_ACCOUNT="eod-swing-runtime"

echo "=== India Swing Trading System Deployment Configuration ==="
echo "Project ID:       ${PROJECT_ID}"
echo "Region:           ${REGION}"
echo "Registry:         ${REPOSITORY}"
echo "Image Name:       ${IMAGE_NAME}:${TAG}"
echo "Storage Bucket:   gs://${BUCKET_NAME}"
echo "Firestore Db:     ${FIRESTORE_DATABASE}"
echo "Run Service:      ${SERVICE_NAME}"
echo "Run Job:          ${JOB_NAME}"
echo "EOD Scheduler:    no automated activation supported; always paused/disabled"
echo "==========================================================="

# ------------------------------------------------------------------------------
# 2. Enable Required Google Cloud APIs
# ------------------------------------------------------------------------------
echo "Enabling GCP Services..."
gcloud services enable \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  firestore.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  aiplatform.googleapis.com \
  cloudbuild.googleapis.com

# ------------------------------------------------------------------------------
# 3. Create GCS Regional Bucket & Firestore
# ------------------------------------------------------------------------------
echo "Provisioning regional Cloud Storage Bucket (gs://${BUCKET_NAME})..."
if ! gsutil ls -b "gs://${BUCKET_NAME}" &>/dev/null; then
  # Create regional bucket in asia-south1
  gsutil mb -c regional -l "${REGION}" "gs://${BUCKET_NAME}"
  
  # Enforce Uniform Bucket-Level Access for security
  gcloud storage buckets update "gs://${BUCKET_NAME}" --uniform-bucket-level-access
  
  # Enable Object Versioning for point-in-time safety
  gcloud storage buckets update "gs://${BUCKET_NAME}" --versioning
  echo "Cloud Storage Bucket created and configured successfully."
else
  echo "Cloud Storage Bucket already exists. Skipping creation."
fi

echo "Provisioning Firestore Database..."
# Attempt to create Firestore in Native mode (uses standard default database)
if ! gcloud firestore databases describe --database="${FIRESTORE_DATABASE}" &>/dev/null; then
  gcloud firestore databases create \
    --location="${REGION}" \
    --type=firestore-native \
    --database="${FIRESTORE_DATABASE}" || true
  echo "Firestore database creation initiated."
else
  echo "Firestore database already exists. Skipping."
fi

# ------------------------------------------------------------------------------
# 4. Configure Service Accounts
# ------------------------------------------------------------------------------
echo "Creating Service Accounts..."
for sa in "${SCHEDULER_SERVICE_ACCOUNT}" "${JOB_SCHEDULER_SERVICE_ACCOUNT}" "${COLLECTOR_RUNTIME_SERVICE_ACCOUNT}" "${JOB_RUNTIME_SERVICE_ACCOUNT}"; do
  if ! gcloud iam service-accounts describe "${sa}@${PROJECT_ID}.iam.gserviceaccount.com" &>/dev/null; then
    gcloud iam service-accounts create "${sa}" \
      --display-name="Service Account for ${sa}"
    echo "Created service account: ${sa}"
  else
    echo "Service account ${sa} already exists. Skipping."
  fi
done

# ------------------------------------------------------------------------------
# 5. Provision Secret Manager Secrets
# ------------------------------------------------------------------------------
echo "Configuring Secret Manager Secrets..."
SECRETS=(
  "KITE_API_KEY"
  "KITE_API_SECRET"
  "LLM_API_KEY"
  "NOTIFICATION_TOKEN"
)

for secret in "${SECRETS[@]}"; do
  if ! gcloud secrets describe "${secret}" &>/dev/null; then
    gcloud secrets create "${secret}" \
      --replication-policy="automatic"
    echo "Created Secret: ${secret}"
  else
    echo "Secret ${secret} already exists."
  fi

  # Ensure at least one placeholder version exists so that Cloud Run can resolve the 'latest' version
  if [[ -z "$(gcloud secrets versions list "${secret}" --limit=1 --format="value(name)" 2>/dev/null)" ]]; then
    echo "PLACEHOLDER" | gcloud secrets versions add "${secret}" --data-file=- &>/dev/null
    echo "Added placeholder version for Secret: ${secret}"
  fi
done

# ------------------------------------------------------------------------------
# 5a. Verify the pre-existing operator-authored pinned run-spec secret
# ------------------------------------------------------------------------------
# This control secret is never created, seeded, or given a placeholder
# version by this script -- it must already exist with the exact pinned
# version enabled. This script reads neither its bytes nor a hash of them.
echo "Verifying pinned run-spec secret '${PINNED_RUN_SPEC_SECRET_NAME}' version ${PINNED_GCS_RUN_SPEC_SECRET_VERSION}..."
if ! gcloud secrets describe "${PINNED_RUN_SPEC_SECRET_NAME}" &>/dev/null; then
  echo "Error: Secret Manager secret '${PINNED_RUN_SPEC_SECRET_NAME}' does not exist. Create it and add the pinned run-spec version out of band before running this script; this script never creates or seeds this control secret." >&2
  exit 1
fi
PINNED_RUN_SPEC_VERSION_STATE="$(gcloud secrets versions describe "${PINNED_GCS_RUN_SPEC_SECRET_VERSION}" --secret="${PINNED_RUN_SPEC_SECRET_NAME}" --format="value(state)" 2>/dev/null || echo "")"
if [ "${PINNED_RUN_SPEC_VERSION_STATE}" != "ENABLED" ]; then
  echo "Error: Secret Manager secret '${PINNED_RUN_SPEC_SECRET_NAME}' version ${PINNED_GCS_RUN_SPEC_SECRET_VERSION} does not exist or is not ENABLED. This script never chooses a version automatically or falls back to 'latest'." >&2
  exit 1
fi

# ------------------------------------------------------------------------------
# 6. Apply IAM Role Bindings (Least Privilege)
# ------------------------------------------------------------------------------
echo "Applying IAM Role Bindings..."

# A. RSS Collector Runtime Permissions
# Grant GCS regional bucket object read/write (storage.objectUser)
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${COLLECTOR_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"

# Grant Firestore read/write (datastore.user)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COLLECTOR_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/datastore.user"

# B. EOD Swing Runtime Permissions
# Grant GCS regional bucket object read/write
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${JOB_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"

# Grant Firestore read/write
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${JOB_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/datastore.user"

# Grant Vertex AI permission for TradingAgents LLM review
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${JOB_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/aiplatform.user"

# Grant Access to Secret Manager secrets: least privilege. The eod-swing
# job's cloud_job.py entrypoint reads no Kite/LLM/notification capability,
# so KITE_API_KEY, KITE_API_SECRET, LLM_API_KEY, and NOTIFICATION_TOKEN
# remain provisioned above for future, separately reviewed features but
# are deliberately never granted or mounted here. The runtime service
# account gets accessor on exactly one secret: the operator-authored
# pinned run-spec control document.
gcloud secrets add-iam-policy-binding "${PINNED_RUN_SPEC_SECRET_NAME}" \
  --member="serviceAccount:${JOB_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Revoke bindings that older versions of this deployment script granted to
# the current job runtime. Merely omitting them from the new job definition
# does not remove secret-level IAM already persisted in GCP. Fail closed if a
# revocation cannot be applied; otherwise the job would retain an unused
# capability contrary to the least-privilege contract above.
for secret in "${SECRETS[@]}"; do
  gcloud secrets remove-iam-policy-binding "${secret}" \
    --member="serviceAccount:${JOB_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet
done

# C. Cloud Build and Compute Engine default service account permissions
# Scoped to the specific bucket (storage.admin) only — NOT project-wide.
# Artifact Registry writer bindings are applied after the repository is
# created in Section 7, scoped to that repository only.
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")

echo "Granting bucket-scoped storage permissions to Compute Engine and Cloud Build service accounts..."
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/storage.admin"

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/storage.admin"


# ------------------------------------------------------------------------------
# 7. Provision Artifact Registry & Build/Push Image
# ------------------------------------------------------------------------------
echo "Configuring Artifact Registry..."
if ! gcloud artifacts repositories describe "${REPOSITORY}" --location="${REGION}" &>/dev/null; then
  gcloud artifacts repositories create "${REPOSITORY}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Docker repository for India Swing Trading System"
  echo "Artifact Registry repository '${REPOSITORY}' created."
else
  echo "Artifact Registry repository already exists."
fi

echo "Granting repository-scoped Artifact Registry permissions to Compute Engine and Cloud Build service accounts..."
gcloud artifacts repositories add-iam-policy-binding "${REPOSITORY}" \
  --location="${REGION}" \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud artifacts repositories add-iam-policy-binding "${REPOSITORY}" \
  --location="${REGION}" \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

# Configure Docker Authentication for regional GCR/GAR
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

FULL_IMAGE_URL="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"

echo "Building and Pushing Docker Image to Artifact Registry..."
# Build locally or via Cloud Build
# Using standard docker build if running on a machine with docker daemon, else fall back or instruct.
if command -v docker &>/dev/null && docker info &>/dev/null; then
  echo "Local Docker Daemon found. Building image locally..."
  docker build -t "${FULL_IMAGE_URL}" .
  docker push "${FULL_IMAGE_URL}"
else
  echo "No local Docker Daemon found or running. Building via Cloud Build..."
  gcloud builds submit --tag "${FULL_IMAGE_URL}" .
fi

# ------------------------------------------------------------------------------
# 8. Deploy 'rss-collector' Cloud Run Service
# ------------------------------------------------------------------------------
# DISABLED: the real rss-collector application entrypoint does not exist yet
# (src/ has no rss_collector module, no gunicorn/uvicorn app to run). Deploying
# a `python -m http.server` placeholder would ship a bare file server on an
# authenticated Cloud Run service, which is not acceptable even behind auth.
# Re-enable this block once a real entrypoint (e.g. gunicorn/uvicorn) exists,
# and replace --command/--args accordingly.
#
# echo "Deploying Cloud Run Service: ${SERVICE_NAME}..."
# gcloud run deploy "${SERVICE_NAME}" \
#   --image="${FULL_IMAGE_URL}" \
#   --region="${REGION}" \
#   --min-instances=0 \
#   --max-instances=1 \
#   --cpu=1 \
#   --memory=512Mi \
#   --timeout=120s \
#   --ingress=all \
#   --no-allow-unauthenticated \
#   --service-account="${COLLECTOR_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
#   --set-env-vars="BUCKET_NAME=${BUCKET_NAME},FIRESTORE_DATABASE=${FIRESTORE_DATABASE}" \
#   --command="<real-entrypoint>" \
#   --args="<real-args>"
#
# # Get the Service URL
# SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" --region="${REGION}" --format='value(status.url)')
# echo "Cloud Run Service deployed at: ${SERVICE_URL}"
#
# # Allow Scheduler S.A. to invoke the service
# gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
#   --region="${REGION}" \
#   --member="serviceAccount:${SCHEDULER_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
#   --role="roles/run.invoker"

# ------------------------------------------------------------------------------
# 9. Deploy 'eod-swing' Cloud Run Job
# ------------------------------------------------------------------------------
echo "Deploying Cloud Run Job: ${JOB_NAME}..."
# Job configuration: 1 Task, 1 Retry, 2 CPU, 4GiB memory, 90 mins task timeout.
# The job's sole run-spec authority is the pinned Secret Manager version
# mounted as a file at PINNED_RUN_SPEC_MOUNT_PATH and passed through
# --spec-file; there is no environment-variable secret, GCS FUSE mount, or
# downloaded temporary spec. Every INDIA_SWING_*_ROOT below is a distinct
# path under the job container's own /tmp/india-swing, which is ephemeral
# (lost when the task ends) and suitable only for a bounded manual
# validation run until real cloud artifact-store persistence exists.
PINNED_JOB_ARTIFACT_ROOTS="INDIA_SWING_CALENDAR_DATA_ROOT=/tmp/india-swing/calendar_data,INDIA_SWING_IDENTITY_REGISTRY_ROOT=/tmp/india-swing/identity_registry,INDIA_SWING_HISTORICAL_PRICES_ROOT=/tmp/india-swing/historical_prices,INDIA_SWING_DAILY_REPORTS_ROOT=/tmp/india-swing/daily_reports,INDIA_SWING_REFERENCE_DATA_ROOT=/tmp/india-swing/reference_data,INDIA_SWING_DAILY_PIPELINE_ROOT=/tmp/india-swing/daily_pipeline"
if gcloud run jobs describe "${JOB_NAME}" --region="${REGION}" &>/dev/null; then
  echo "Job exists, updating..."
  gcloud run jobs update "${JOB_NAME}" \
    --image="${FULL_IMAGE_URL}" \
    --region="${REGION}" \
    --command=python \
    --args=-m,india_swing.cloud_job,--spec-file,"${PINNED_RUN_SPEC_MOUNT_PATH}" \
    --tasks=1 \
    --max-retries=1 \
    --cpu=2 \
    --memory=4Gi \
    --task-timeout=5400s \
    --service-account="${JOB_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --set-env-vars="BUCKET_NAME=${BUCKET_NAME},FIRESTORE_DATABASE=${FIRESTORE_DATABASE},${PINNED_JOB_ARTIFACT_ROOTS}" \
    --set-secrets="${PINNED_RUN_SPEC_MOUNT_PATH}=${PINNED_RUN_SPEC_SECRET_NAME}:${PINNED_GCS_RUN_SPEC_SECRET_VERSION}"
else
  echo "Creating new Job..."
  gcloud run jobs create "${JOB_NAME}" \
    --image="${FULL_IMAGE_URL}" \
    --region="${REGION}" \
    --command=python \
    --args=-m,india_swing.cloud_job,--spec-file,"${PINNED_RUN_SPEC_MOUNT_PATH}" \
    --tasks=1 \
    --max-retries=1 \
    --cpu=2 \
    --memory=4Gi \
    --task-timeout=5400s \
    --service-account="${JOB_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --set-env-vars="BUCKET_NAME=${BUCKET_NAME},FIRESTORE_DATABASE=${FIRESTORE_DATABASE},${PINNED_JOB_ARTIFACT_ROOTS}" \
    --set-secrets="${PINNED_RUN_SPEC_MOUNT_PATH}=${PINNED_RUN_SPEC_SECRET_NAME}:${PINNED_GCS_RUN_SPEC_SECRET_VERSION}"
fi

# Allow Scheduler S.A. to run the job
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" \
  --member="serviceAccount:${JOB_SCHEDULER_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

# ------------------------------------------------------------------------------
# 10. Provision Cloud Scheduler Crons
# ------------------------------------------------------------------------------
echo "Configuring Cloud Scheduler Jobs..."

# A. RSS Collector Schedule: Every 5 minutes
# DISABLED: depends on SERVICE_URL from the rss-collector Cloud Run Service
# deployment in Section 8, which is disabled until a real entrypoint exists.
# Re-enable together with Section 8.
#
# if gcloud scheduler jobs describe "rss-collector-schedule" --location="${REGION}" &>/dev/null; then
#   echo "Updating existing RSS Scheduler Job..."
#   gcloud scheduler jobs update http rss-collector-schedule \
#     --location="${REGION}" \
#     --schedule="*/5 * * * *" \
#     --time-zone="Asia/Kolkata" \
#     --uri="${SERVICE_URL}/" \
#     --http-method=GET \
#     --oidc-service-account-email="${SCHEDULER_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
#     --oidc-token-audience="${SERVICE_URL}/"
# else
#   echo "Creating new RSS Scheduler Job..."
#   gcloud scheduler jobs create http rss-collector-schedule \
#     --location="${REGION}" \
#     --schedule="*/5 * * * *" \
#     --time-zone="Asia/Kolkata" \
#     --uri="${SERVICE_URL}/" \
#     --http-method=GET \
#     --oidc-service-account-email="${SCHEDULER_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
#     --oidc-token-audience="${SERVICE_URL}/"
# fi

# B. EOD Swing Job Schedule: no automated-activation path exists anywhere
# in this script. A single static pinned run-spec targets one exact market
# session and previous-run binding; scheduling it every weekday would
# replay the same stale inputs unattended. ENABLE_EOD_SCHEDULER=true is
# already rejected in Section 1 before any GCP mutation is reachable, so
# by this point ENABLE_EOD_SCHEDULER is guaranteed not to be "true". There
# is no gcloud scheduler jobs create/update/resume call for
# eod-swing-schedule anywhere in this script -- only pause-if-present, or
# report disabled otherwise.
if gcloud scheduler jobs describe "eod-swing-schedule" --location="${REGION}" &>/dev/null; then
  echo "Pausing the existing EOD Swing Scheduler Job (no automated activation is supported)..."
  gcloud scheduler jobs pause "eod-swing-schedule" --location="${REGION}"
  EOD_SCHEDULER_STATE="paused"
else
  echo "EOD Swing Scheduler Job remains disabled (not created)."
  EOD_SCHEDULER_STATE="disabled"
fi

echo "=========================================================="
echo "SUCCESS: GCP Infrastructure provisioning pipeline configured!"
echo "Check the Secret Manager console to populate your keys."
echo "EOD Swing Scheduler: ${EOD_SCHEDULER_STATE} (ENABLE_EOD_SCHEDULER=${ENABLE_EOD_SCHEDULER})"
echo "RSS Collector Scheduler: disabled (rss-collector service is not yet deployed)."
echo "=========================================================="
