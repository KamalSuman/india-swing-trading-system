$rawInput = [Console]::In.ReadToEnd()

function Write-Decision {
    param(
        [Parameter(Mandatory = $true)][string]$Decision,
        [Parameter(Mandatory = $true)][string]$Reason
    )
    $result = [ordered]@{ decision = $Decision; reason = $Reason }
    [Console]::Out.Write(($result | ConvertTo-Json -Compress))
    exit 0
}

try {
    $event = $rawInput | ConvertFrom-Json
} catch {
    Write-Decision -Decision "deny" -Reason "Antigravity outbox gate received malformed tool input."
}

if ([string]$event.toolCall.name -ne "write_to_file") {
    Write-Decision -Decision "deny" -Reason "Antigravity may use only the guarded outbox write operation."
}

$target = [string]$event.toolCall.args.TargetFile
$content = [string]$event.toolCall.args.CodeContent
$overwrite = $event.toolCall.args.Overwrite
if ([string]::IsNullOrWhiteSpace($target) -or [string]::IsNullOrWhiteSpace($content)) {
    Write-Decision -Decision "deny" -Reason "Antigravity outbox target and JSON content are required."
}
if ($overwrite -ne $true) {
    Write-Decision -Decision "deny" -Reason "Antigravity must overwrite only its existing outbox file."
}
if ($content.Length -gt 131072) {
    Write-Decision -Decision "deny" -Reason "Antigravity outbox exceeds the 128 KiB safety limit."
}

$matchedRoot = $null
foreach ($workspaceRoot in @($event.workspacePaths)) {
    try {
        $root = [IO.Path]::GetFullPath([string]$workspaceRoot)
        $allowed = [IO.Path]::GetFullPath(
            (Join-Path $root "agent-control\outbox\antigravity-response.json")
        )
        if ([IO.Path]::IsPathRooted($target)) {
            $candidate = [IO.Path]::GetFullPath($target)
        } else {
            $candidate = [IO.Path]::GetFullPath((Join-Path $root $target))
        }
        if ([string]::Equals($candidate, $allowed, [StringComparison]::OrdinalIgnoreCase)) {
            $matchedRoot = $root
            break
        }
    } catch {
        continue
    }
}
if ($null -eq $matchedRoot) {
    Write-Decision -Decision "deny" -Reason "Antigravity may write only agent-control/outbox/antigravity-response.json."
}

try {
    $response = $content | ConvertFrom-Json
} catch {
    Write-Decision -Decision "deny" -Reason "Antigravity outbox must contain valid JSON."
}

$requiredKeys = @(
    "schema_version", "task_id", "task_revision", "agent", "status",
    "summary", "files_read", "files_changed", "commands_run", "tests_run",
    "findings", "assumptions", "confirmations"
)
$actualKeys = @($response.PSObject.Properties.Name)
if ($actualKeys.Count -ne $requiredKeys.Count) {
    Write-Decision -Decision "deny" -Reason "Antigravity outbox has an invalid top-level schema."
}
foreach ($key in $requiredKeys) {
    if ($actualKeys -notcontains $key) {
        Write-Decision -Decision "deny" -Reason "Antigravity outbox is missing a required field."
    }
}

$taskPath = Join-Path $matchedRoot "agent-control\inbox\antigravity-task.json"
try {
    $task = Get-Content -LiteralPath $taskPath -Raw | ConvertFrom-Json
} catch {
    Write-Decision -Decision "deny" -Reason "Antigravity task inbox could not be validated."
}

if ($response.schema_version -ne 1 -or $response.agent -ne "ANTIGRAVITY") {
    Write-Decision -Decision "deny" -Reason "Antigravity outbox identity is invalid."
}
if ($response.task_id -ne $task.task_id -or $response.task_revision -ne $task.revision) {
    Write-Decision -Decision "deny" -Reason "Antigravity outbox does not match the active task revision."
}
if (@("COMPLETE", "FINDINGS_ONLY", "BLOCKED") -notcontains $response.status) {
    Write-Decision -Decision "deny" -Reason "Antigravity outbox status is invalid."
}
if (@($response.files_changed).Count -ne 0 -or
    @($response.commands_run).Count -ne 0 -or
    @($response.tests_run).Count -ne 0) {
    Write-Decision -Decision "deny" -Reason "Antigravity is read-only and cannot report file changes, commands, or tests."
}

$confirmationKeys = @(
    "no_unlisted_files_changed",
    "no_commit_push_deploy",
    "no_broker_cloud_live_store_mutation"
)
foreach ($key in $confirmationKeys) {
    if ($response.confirmations.$key -ne $true) {
        Write-Decision -Decision "deny" -Reason "Antigravity outbox safety confirmations are incomplete."
    }
}

Write-Decision -Decision "allow" -Reason "Schema-valid Antigravity handoff may overwrite its exact outbox file."
