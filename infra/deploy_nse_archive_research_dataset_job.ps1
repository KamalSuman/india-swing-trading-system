# Deploy the one-shot, non-actionable NSE archive research-dataset builder.
#
# The source corpus is read from a Cloud Storage volume and the canonical
# content-addressed dataset manifest is published through the GCS API.  This
# script creates no scheduler and does not execute the job.

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Require-Value([string]$Name) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Required research-dataset deployment value is missing."
    }
    return $value
}

$projectId = Require-Value "GCP_PROJECT_ID"
$region = if ($env:GCP_REGION) { $env:GCP_REGION } else { "asia-south1" }
$jobName = if ($env:INDIA_SWING_RESEARCH_DATASET_JOB_NAME) { $env:INDIA_SWING_RESEARCH_DATASET_JOB_NAME } else { "india-swing-nse-archive-research-dataset" }
$serviceAccount = Require-Value "INDIA_SWING_RESEARCH_DATASET_SERVICE_ACCOUNT"
$bucket = Require-Value "INDIA_SWING_RESEARCH_DATASET_BUCKET"
$image = Require-Value "INDIA_SWING_IMAGE_DIGEST_URI"

if ($image -notmatch '@sha256:[0-9a-f]{64}$') {
    throw "Research-dataset image must be pinned by sha256 digest."
}

$indexIds = @(
    "c12521043af92ea79643eb63df4970af062f8ee9a393094025a94f2f8d7c9e33",
    "f54cf9284de889dab27770c5f0c3bfdec2249057ab7794e2725cd2b439f85a5c",
    "fbbc8e515029547a86be748d38eb2bbd9d82adc83fbdff22e173dae24144151d",
    "7846f13efa327827befbe5233117ecf0989ccb47c1611fdf2cd5417d52075860",
    "b41cfca1311c90200bd8436d57412a1318684ba84d07aeddcdc58464e22b2515",
    "ad69b8254d62a9053e0f2b043f1bfb7adc7650abf5a0b7a3fe2b65c19c980d79",
    "c3a1da8a9978e95ec632f7029d5f8be1a1d7b95254248da7afb00b09893964cc",
    "858458c7a4e1bba740d5483a2950041f01a2104075780bb4cb82baf07d430b64",
    "8e8acad7ff2cf1b09b6ac0f3bbb81154ab41381e9da8e5b2d06ea6e25f8296fe"
)

$jobArguments = @(
    "-m",
    "india_swing.evaluation.nse_archive_research_dataset_cloud_job",
    "--store-root",
    "/mnt/india-swing-data/research/canonical-market-data",
    "--bucket",
    $bucket
)
foreach ($indexId in $indexIds) {
    $jobArguments += @("--index-snapshot-id", $indexId)
}
$jobArguments += @(
    "--train-end", "2022-12-31",
    "--validation-start", "2023-01-01",
    "--validation-end", "2024-12-31",
    "--test-start", "2025-01-01",
    "--maximum-forward-label-horizon-sessions", "20"
)

& gcloud storage buckets add-iam-policy-binding "gs://$bucket" `
    --project=$projectId `
    --member="serviceAccount:$serviceAccount" `
    --role="roles/storage.objectUser" `
    --quiet
if ($LASTEXITCODE -ne 0) { throw "Research-dataset bucket IAM failed." }

$common = @(
    "--image=$image",
    "--region=$region",
    "--project=$projectId",
    "--service-account=$serviceAccount",
    "--command=python",
    "--args=$($jobArguments -join ',')",
    "--tasks=1",
    "--max-retries=0",
    "--cpu=8",
    "--memory=16Gi",
    "--task-timeout=7200s",
    "--execution-environment=gen2",
    "--add-volume=name=corpus,type=cloud-storage,bucket=$bucket",
    "--add-volume-mount=volume=corpus,mount-path=/mnt/india-swing-data",
    "--quiet"
)

& gcloud run jobs describe $jobName --region=$region --project=$projectId *> $null
if ($LASTEXITCODE -eq 0) {
    & gcloud run jobs update $jobName @common
} else {
    & gcloud run jobs create $jobName @common
}
if ($LASTEXITCODE -ne 0) { throw "Research-dataset Cloud Run Job deployment failed." }

Write-Output '{"scheduler":"ABSENT","status":"NSE_ARCHIVE_RESEARCH_DATASET_JOB_DEPLOYED"}'
