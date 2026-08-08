[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateRange(2000, 2100)]
    [int]$Year,

    [Parameter(Mandatory = $true)]
    [string]$DownloadDirectory,

    [Parameter(Mandatory = $true)]
    [string]$DataRoot
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.IO.Compression.FileSystem

function Get-Sha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Copy-VerifiedFile([string]$Source, [string]$Destination) {
    if (Test-Path -LiteralPath $Destination) {
        if ((Get-Sha256 $Source) -ne (Get-Sha256 $Destination)) {
            throw "Existing staged file disagrees with source evidence"
        }
        return
    }
    Copy-Item -LiteralPath $Source -Destination $Destination
}

function Read-ZipEntryText([IO.Compression.ZipArchiveEntry]$Entry) {
    # throwOnInvalidBytes: $true so malformed UTF-8 raises rather than being
    # silently replacement-decoded; detectEncodingFromByteOrderMarks: $true
    # so a UTF-8 BOM is stripped rather than rejected or treated as content.
    $stream = $Entry.Open()
    try {
        $strictUtf8 = New-Object System.Text.UTF8Encoding($false, $true)
        $reader = New-Object IO.StreamReader($stream, $strictUtf8, $true)
        try {
            return $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }
    }
    finally {
        $stream.Dispose()
    }
}

function ConvertTo-StrictCsvRows([string]$Text) {
    # Returns $null on any malformed quoting or an unterminated quoted
    # field. Each row is a string[]; the list itself is returned wrapped
    # (",$rows") so a single-row result is not unwrapped by the pipeline.
    $rows = New-Object 'System.Collections.Generic.List[string[]]'
    $row = New-Object 'System.Collections.Generic.List[string]'
    $field = New-Object System.Text.StringBuilder
    $inQuotes = $false
    # True immediately after a quoted field's closing quote, until the next
    # delimiter/newline/end-of-input is consumed. Anything else seen while
    # this is true (an ordinary character, whitespace, or another quote)
    # means content trails the closing quote, which is malformed.
    $justClosedQuote = $false
    $length = $Text.Length
    $index = 0

    while ($index -lt $length) {
        $ch = $Text[$index]
        if ($inQuotes) {
            if ($ch -eq '"') {
                if (($index + 1) -lt $length -and $Text[$index + 1] -eq '"') {
                    [void]$field.Append('"')
                    $index += 2
                }
                else {
                    $inQuotes = $false
                    $justClosedQuote = $true
                    $index++
                }
            }
            else {
                [void]$field.Append($ch)
                $index++
            }
            continue
        }

        if ($justClosedQuote -and $ch -ne ',' -and $ch -ne "`r" -and $ch -ne "`n") {
            return $null
        }

        if ($ch -eq '"') {
            if ($field.Length -ne 0) {
                return $null
            }
            $inQuotes = $true
            $index++
            continue
        }
        if ($ch -eq ',') {
            $row.Add($field.ToString())
            [void]$field.Clear()
            $justClosedQuote = $false
            $index++
            continue
        }
        if ($ch -eq "`r") {
            $row.Add($field.ToString())
            [void]$field.Clear()
            $rows.Add($row.ToArray())
            $row = New-Object 'System.Collections.Generic.List[string]'
            $justClosedQuote = $false
            if (($index + 1) -lt $length -and $Text[$index + 1] -eq "`n") {
                $index += 2
            }
            else {
                $index++
            }
            continue
        }
        if ($ch -eq "`n") {
            $row.Add($field.ToString())
            [void]$field.Clear()
            $rows.Add($row.ToArray())
            $row = New-Object 'System.Collections.Generic.List[string]'
            $justClosedQuote = $false
            $index++
            continue
        }

        [void]$field.Append($ch)
        $index++
    }

    if ($inQuotes) {
        return $null
    }
    if ($field.Length -gt 0 -or $row.Count -gt 0) {
        $row.Add($field.ToString())
        $rows.Add($row.ToArray())
    }

    return ,$rows
}

function Test-CanonicalFullBhavcopySession {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$Text,

        [Parameter(Mandatory = $true)]
        [string]$ExpectedSessionText
    )

    if ([string]::IsNullOrEmpty($Text)) {
        return $false
    }

    $rows = ConvertTo-StrictCsvRows $Text
    if ($null -eq $rows -or $rows.Count -eq 0) {
        return $false
    }

    $lastRow = $rows[$rows.Count - 1]
    if ($lastRow.Count -eq 1 -and $lastRow[0] -eq "") {
        $rows.RemoveAt($rows.Count - 1)
    }
    if ($rows.Count -lt 2) {
        return $false
    }

    $header = @($rows[0] | ForEach-Object { $_.Trim() })
    $distinctHeader = @($header | Select-Object -Unique)
    if ($distinctHeader.Count -ne $header.Count) {
        return $false
    }
    $dateIndices = @(
        0..($header.Count - 1) | Where-Object { $header[$_] -eq "DATE1" }
    )
    if ($dateIndices.Count -ne 1) {
        return $false
    }
    $dateIndex = $dateIndices[0]

    for ($rowIndex = 1; $rowIndex -lt $rows.Count; $rowIndex++) {
        $row = $rows[$rowIndex]
        if ($row.Count -ne $header.Count) {
            return $false
        }
        $dateValue = $row[$dateIndex].Trim()
        $parsedDate = [datetime]::MinValue
        $parsedOk = [datetime]::TryParseExact(
            $dateValue,
            "dd-MMM-yyyy",
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::None,
            [ref]$parsedDate
        )
        if (-not $parsedOk) {
            return $false
        }
        $normalized = $parsedDate.ToString(
            "dd-MMM-yyyy",
            [Globalization.CultureInfo]::InvariantCulture
        )
        if ($normalized -ne $ExpectedSessionText) {
            return $false
        }
    }

    return $true
}

$LegacyMonthAbbreviations = @(
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"
)

$downloads = [IO.Path]::GetFullPath($DownloadDirectory)
$data = [IO.Path]::GetFullPath($DataRoot)
$sourceRoot = [IO.Path]::GetFullPath((Join-Path $data "source-archives"))
$stagingRoot = [IO.Path]::GetFullPath((Join-Path $data "staging"))
$quarantineRoot = [IO.Path]::GetFullPath(
    (Join-Path $data "quarantine\nse-historical-archive")
)

if (-not (Test-Path -LiteralPath $downloads -PathType Container)) {
    throw "Download directory does not exist"
}
foreach ($root in @($sourceRoot, $stagingRoot, $quarantineRoot)) {
    if (-not $root.StartsWith($data, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Resolved data root is unsafe"
    }
    New-Item -ItemType Directory -Path $root -Force | Out-Null
}

$namePattern = "^Reports-Archives-Multiple-(\d{2})(\d{2})($Year)\.zip$"
$archives = @(
    Get-ChildItem -LiteralPath $downloads -File |
        Where-Object { $_.Name -match $namePattern } |
        Sort-Object Name
)
if ($archives.Count -eq 0) {
    throw "No canonical NSE archive files were found for the requested year"
}

$validated = 0
$quarantined = 0
$extracted = 0
$alreadyPresent = 0

foreach ($archive in $archives) {
    if ($archive.Name -notmatch $namePattern) {
        throw "Archive filename changed during staging"
    }
    $day = $Matches[1]
    $month = $Matches[2]
    $yearText = $Matches[3]
    $sessionDate = [datetime]::ParseExact(
        "$yearText-$month-$day",
        "yyyy-MM-dd",
        [Globalization.CultureInfo]::InvariantCulture
    )
    $session = $sessionDate.ToString("yyyy-MM-dd")
    $shortYear = $yearText.Substring(2, 2)
    $fullName = "sec_bhavdata_full_${day}${month}${yearText}.csv"
    $udiffName = "BhavCopy_NSE_CM_0_0_0_${yearText}${month}${day}_F_0000.csv.zip"
    $reg1Name = "REG1_IND${day}${month}${shortYear}.csv"
    $securityName = "NSE_CM_security_${day}${month}${yearText}.csv.gz"
    # Locale-independent: never depends on ToString("MMM"), which is bound
    # to the current culture.
    $legacyMonthAbbreviation = $LegacyMonthAbbreviations[$sessionDate.Month - 1]
    $legacyZipName = "cm${day}${legacyMonthAbbreviation}${yearText}bhav.csv.zip"
    $mtoName = "MTO_${day}${month}${yearText}.DAT"
    $oneFileSignature = (@($fullName) | Sort-Object) -join "|"
    $legacyPairSignature = (@($legacyZipName, $mtoName) | Sort-Object) -join "|"

    $zip = [IO.Compression.ZipFile]::OpenRead($archive.FullName)
    try {
        $entries = @($zip.Entries)
        $names = @($entries | ForEach-Object { $_.FullName })
        if (@($names | Select-Object -Unique).Count -ne $names.Count) {
            throw "Archive contains duplicate entry names"
        }
        $signature = (@($names | Sort-Object) -join "|")
        $accepted = @(
            $oneFileSignature
            (@($fullName, $udiffName) | Sort-Object) -join "|"
            (@($fullName, $udiffName, $securityName) | Sort-Object) -join "|"
            (@($fullName, $udiffName, $reg1Name, $securityName) | Sort-Object) -join "|"
            $legacyPairSignature
        )

        $isUnsupported = ($accepted -notcontains $signature)
        if (-not $isUnsupported -and $signature -eq $oneFileSignature) {
            # Single full-Bhavcopy wrapper: validate the entry in memory,
            # before any source-archive or staging directory is created,
            # so a stale (e.g. holiday-preceding-session) wrapper can only
            # ever reach quarantine.
            $expectedSessionText = $sessionDate.ToString(
                "dd-MMM-yyyy",
                [Globalization.CultureInfo]::InvariantCulture
            )
            $oneFileEntry = $entries |
                Where-Object { $_.FullName -eq $fullName } |
                Select-Object -First 1

            if ($oneFileEntry.Length -le 0 -or $oneFileEntry.Length -gt 64MB) {
                # Empty or larger than the production entry ceiling
                # (MAXIMUM_ENTRY_BYTES): quarantined without opening or
                # reading its content.
                $isUnsupported = $true
            }
            else {
                $oneFileText = $null
                try {
                    $oneFileText = Read-ZipEntryText $oneFileEntry
                }
                catch {
                    # Invalid UTF-8 (or any other decode failure): treat as
                    # an unsupported wrapper rather than letting the error
                    # abort the whole year.
                    $oneFileText = $null
                }
                if ($null -eq $oneFileText -or
                    -not (Test-CanonicalFullBhavcopySession $oneFileText $expectedSessionText)) {
                    $isUnsupported = $true
                }
            }
        }

        if ($isUnsupported) {
            $quarantineDirectory = Join-Path $quarantineRoot $session
            New-Item -ItemType Directory -Path $quarantineDirectory -Force |
                Out-Null
            $quarantineName = "$($archive.BaseName)-unsupported-or-stale-wrapper.zip"
            Copy-VerifiedFile `
                $archive.FullName `
                (Join-Path $quarantineDirectory $quarantineName)
            $quarantined++
            continue
        }

        $archiveDirectory = Join-Path $sourceRoot $session
        $stagingDirectory = Join-Path $stagingRoot $session
        New-Item -ItemType Directory -Path $archiveDirectory -Force | Out-Null
        New-Item -ItemType Directory -Path $stagingDirectory -Force | Out-Null

        $archiveDestination = Join-Path $archiveDirectory $archive.Name
        $wasPresent = Test-Path -LiteralPath $archiveDestination
        Copy-VerifiedFile $archive.FullName $archiveDestination
        if ($wasPresent) {
            $alreadyPresent++
        }

        foreach ($entry in $entries) {
            if ([IO.Path]::GetFileName($entry.FullName) -ne $entry.FullName) {
                throw "Archive entry is not a safe basename"
            }
            $entryDestination = Join-Path $stagingDirectory $entry.FullName
            if (Test-Path -LiteralPath $entryDestination) {
                $temporary = Join-Path $env:TEMP ([guid]::NewGuid().ToString("N"))
                try {
                    [IO.Compression.ZipFileExtensions]::ExtractToFile(
                        $entry,
                        $temporary,
                        $false
                    )
                    if ((Get-Sha256 $temporary) -ne (Get-Sha256 $entryDestination)) {
                        throw "Existing extracted entry disagrees with source archive"
                    }
                }
                finally {
                    Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
                }
            }
            else {
                [IO.Compression.ZipFileExtensions]::ExtractToFile(
                    $entry,
                    $entryDestination,
                    $false
                )
                $extracted++
            }
        }
        $validated++
    }
    finally {
        $zip.Dispose()
    }
}

[ordered]@{
    status = "NSE_HISTORICAL_YEAR_STAGED"
    year = $Year
    archive_count = $archives.Count
    validated_session_count = $validated
    quarantined_wrapper_count = $quarantined
    extracted_entry_count = $extracted
    already_present_archive_count = $alreadyPresent
    downloads_retained = $true
} | ConvertTo-Json
