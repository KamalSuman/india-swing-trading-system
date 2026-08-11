# Deploy the one-shot, collection-only forward-paper operational graph job.
# This script creates no scheduler and does not execute the job.

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Require-Value([string]$Name) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Required forward-paper deployment value is missing."
    }
    return $value
}

function Require-Version([string]$Name) {
    $value = Require-Value $Name
    if ($value -notmatch '^[1-9][0-9]*$') {
        throw "Forward-paper launch secret version must be exact."
    }
    return $value
}

$projectId = Require-Value "GCP_PROJECT_ID"
$region = if ($env:GCP_REGION) { $env:GCP_REGION } else { "asia-south1" }
$jobName = if ($env:INDIA_SWING_FORWARD_PAPER_JOB_NAME) { $env:INDIA_SWING_FORWARD_PAPER_JOB_NAME } else { "india-swing-forward-paper-operational" }
$serviceAccount = Require-Value "INDIA_SWING_FORWARD_PAPER_SERVICE_ACCOUNT"
$dataBucket = Require-Value "INDIA_SWING_FORWARD_PAPER_DATA_BUCKET"
$stateBucket = Require-Value "INDIA_SWING_FORWARD_PAPER_STATE_BUCKET"
$image = Require-Value "INDIA_SWING_IMAGE_DIGEST_URI"
$launchSecret = Require-Value "INDIA_SWING_FORWARD_PAPER_LAUNCH_SECRET"
$launchVersion = Require-Version "INDIA_SWING_FORWARD_PAPER_LAUNCH_SECRET_VERSION"

if ($image -notmatch '@sha256:[0-9a-f]{64}$') {
    throw "Forward-paper image must be pinned by sha256 digest."
}

$secretState = & gcloud secrets versions describe $launchVersion `
    --secret=$launchSecret --project=$projectId --format="value(state)"
if ($LASTEXITCODE -ne 0 -or $secretState -ne "ENABLED") {
    throw "The exact forward-paper launch secret version is unavailable."
}

foreach ($bucket in @($dataBucket, $stateBucket) | Select-Object -Unique) {
    & gcloud storage buckets add-iam-policy-binding "gs://$bucket" `
        --project=$projectId `
        --member="serviceAccount:$serviceAccount" `
        --role="roles/storage.objectUser" `
        --quiet *> $null
    if ($LASTEXITCODE -ne 0) { throw "Forward-paper bucket IAM failed." }
}

& gcloud secrets add-iam-policy-binding $launchSecret `
    --project=$projectId `
    --member="serviceAccount:$serviceAccount" `
    --role="roles/secretmanager.secretAccessor" `
    --quiet *> $null
if ($LASTEXITCODE -ne 0) { throw "Forward-paper launch-secret IAM failed." }

$launchPath = "/var/run/india-swing/forward-paper-launch.json"
$marketDataRoot = "/mnt/india-swing-data/research/canonical-market-data"
$common = @(
    "--image=$image",
    "--region=$region",
    "--project=$projectId",
    "--service-account=$serviceAccount",
    "--command=python",
    "--args=-m,india_swing.forward_paper.hydrated_cloud_job,--launch-file,$launchPath,--market-data-root,$marketDataRoot",
    "--tasks=1",
    "--max-retries=0",
    "--cpu=4",
    "--memory=8Gi",
    "--task-timeout=3600s",
    "--execution-environment=gen2",
    "--add-volume=name=corpus,type=cloud-storage,bucket=$dataBucket",
    "--add-volume-mount=volume=corpus,mount-path=/mnt/india-swing-data",
    "--set-secrets=$launchPath=$launchSecret`:$launchVersion",
    "--quiet"
)

$existing = & gcloud run jobs list --region=$region --project=$projectId `
    --filter="metadata.name=$jobName" --format="value(metadata.name)"
if ($LASTEXITCODE -ne 0) { throw "Forward-paper Cloud Run Job lookup failed." }
if ($existing -eq $jobName) {
    & gcloud run jobs update $jobName @common
} else {
    & gcloud run jobs create $jobName @common
}
if ($LASTEXITCODE -ne 0) { throw "Forward-paper Cloud Run Job deployment failed." }

Write-Output '{"scheduler":"ABSENT","status":"FORWARD_PAPER_OPERATIONAL_JOB_DEPLOYED"}'
