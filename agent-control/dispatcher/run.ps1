# Launch the mailbox dispatcher. Any arguments are passed through
# (e.g. .\run.ps1 --once --dry-run for a status check).
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$venvPython = Join-Path $PSScriptRoot "..\..\.venv\Scripts\python.exe"
if (Test-Path $venvPython) {
    & $venvPython (Join-Path $PSScriptRoot "dispatcher.py") @args
} else {
    python (Join-Path $PSScriptRoot "dispatcher.py") @args
}
exit $LASTEXITCODE
