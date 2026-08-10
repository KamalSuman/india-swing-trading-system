# Manually deploy the promoted paper-pilot Cloud Run Job.
#
# This script deliberately creates no scheduler.  Every control and secret
# version is exact; "latest" is rejected.  Build/push the image separately and
# pass its immutable digest URI through INDIA_SWING_IMAGE_DIGEST_URI.

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Require-Value([string]$Name) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Required paper-pilot deployment value is missing."
    }
    return $value
}

function Require-Version([string]$Name) {
    $value = Require-Value $Name
    if ($value -notmatch '^[1-9][0-9]*$') {
        throw "Paper-pilot secret version must be an exact positive integer."
    }
    return $value
}

$projectId = Require-Value "GCP_PROJECT_ID"
$region = if ($env:GCP_REGION) { $env:GCP_REGION } else { "asia-south1" }
$jobName = if ($env:INDIA_SWING_PAPER_PILOT_JOB_NAME) { $env:INDIA_SWING_PAPER_PILOT_JOB_NAME } else { "india-swing-promoted-paper-pilot" }
$serviceAccount = Require-Value "INDIA_SWING_PAPER_PILOT_SERVICE_ACCOUNT"
$bucket = Require-Value "INDIA_SWING_PAPER_PILOT_STATE_BUCKET"
$image = Require-Value "INDIA_SWING_IMAGE_DIGEST_URI"

if ($image -notmatch '@sha256:[0-9a-f]{64}$') {
    throw "Paper-pilot image must be pinned by sha256 digest."
}

$launchSecret = Require-Value "INDIA_SWING_PAPER_PILOT_LAUNCH_SECRET"
$launchVersion = Require-Version "INDIA_SWING_PAPER_PILOT_LAUNCH_SECRET_VERSION"
$kiteKeySecret = Require-Value "INDIA_SWING_KITE_API_KEY_SECRET"
$kiteKeyVersion = Require-Version "INDIA_SWING_KITE_API_KEY_SECRET_VERSION"
$kiteTokenSecret = Require-Value "INDIA_SWING_KITE_ACCESS_TOKEN_SECRET"
$kiteTokenVersion = Require-Version "INDIA_SWING_KITE_ACCESS_TOKEN_SECRET_VERSION"
$telegramTokenSecret = Require-Value "INDIA_SWING_TELEGRAM_BOT_TOKEN_SECRET"
$telegramTokenVersion = Require-Version "INDIA_SWING_TELEGRAM_BOT_TOKEN_SECRET_VERSION"
$telegramChatSecret = Require-Value "INDIA_SWING_TELEGRAM_CHAT_ID_SECRET"
$telegramChatVersion = Require-Version "INDIA_SWING_TELEGRAM_CHAT_ID_SECRET_VERSION"

$secretCoordinates = @(
    @($launchSecret, $launchVersion),
    @($kiteKeySecret, $kiteKeyVersion),
    @($kiteTokenSecret, $kiteTokenVersion),
    @($telegramTokenSecret, $telegramTokenVersion),
    @($telegramChatSecret, $telegramChatVersion)
)

foreach ($coordinate in $secretCoordinates) {
    $secretName = [string]$coordinate[0]
    $secretVersion = [string]$coordinate[1]
    $state = & gcloud secrets versions describe $secretVersion `
        --secret=$secretName `
        --project=$projectId `
        --format="value(state)"
    if ($LASTEXITCODE -ne 0 -or $state -ne "ENABLED") {
        throw "An exact paper-pilot secret version is unavailable."
    }
}

# Least privilege for the one runtime identity.  No broker order role exists.
& gcloud storage buckets add-iam-policy-binding "gs://$bucket" `
    --project=$projectId `
    --member="serviceAccount:$serviceAccount" `
    --role="roles/storage.objectUser" `
    --quiet
if ($LASTEXITCODE -ne 0) { throw "Paper-pilot bucket IAM failed." }

foreach ($coordinate in $secretCoordinates) {
    $secretName = [string]$coordinate[0]
    & gcloud secrets add-iam-policy-binding $secretName `
        --project=$projectId `
        --member="serviceAccount:$serviceAccount" `
        --role="roles/secretmanager.secretAccessor" `
        --quiet
    if ($LASTEXITCODE -ne 0) { throw "Paper-pilot secret IAM failed." }
}

$launchPath = "/var/run/india-swing/promoted-launch.json"
$secretBindings = @(
    "$launchPath=$launchSecret`:$launchVersion",
    "INDIA_SWING_KITE_API_KEY=$kiteKeySecret`:$kiteKeyVersion",
    "INDIA_SWING_KITE_ACCESS_TOKEN=$kiteTokenSecret`:$kiteTokenVersion",
    "INDIA_SWING_TELEGRAM_BOT_TOKEN=$telegramTokenSecret`:$telegramTokenVersion",
    "INDIA_SWING_TELEGRAM_CHAT_ID=$telegramChatSecret`:$telegramChatVersion"
) -join ','

$common = @(
    "--image=$image",
    "--region=$region",
    "--project=$projectId",
    "--service-account=$serviceAccount",
    "--command=python",
    "--args=-m,india_swing.promoted_paper_pilot_job,--launch-file,$launchPath",
    "--tasks=1",
    "--max-retries=0",
    "--cpu=2",
    "--memory=4Gi",
    "--task-timeout=3600s",
    "--set-env-vars=INDIA_SWING_PAPER_PILOT_STATE_BUCKET=$bucket",
    "--set-secrets=$secretBindings",
    "--quiet"
)

& gcloud run jobs describe $jobName --region=$region --project=$projectId *> $null
if ($LASTEXITCODE -eq 0) {
    & gcloud run jobs update $jobName @common
} else {
    & gcloud run jobs create $jobName @common
}
if ($LASTEXITCODE -ne 0) { throw "Paper-pilot Cloud Run Job deployment failed." }

# A stale static launch must never be scheduled repeatedly.
$schedulerName = "$jobName-schedule"
& gcloud scheduler jobs describe $schedulerName --location=$region --project=$projectId *> $null
if ($LASTEXITCODE -eq 0) {
    & gcloud scheduler jobs pause $schedulerName --location=$region --project=$projectId --quiet
    if ($LASTEXITCODE -ne 0) { throw "Paper-pilot scheduler could not be paused." }
}

Write-Output '{"scheduler":"DISABLED","status":"PROMOTED_PAPER_PILOT_DEPLOYED"}'
