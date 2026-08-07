# Local operator helpers

## NSE historical archive keyboard downloader

`nse_archive_keyboard_download.ps1` automates only the repeated keyboard
sequence on the normal NSE historical-reports page. It does not navigate to a
hidden endpoint, bypass an access control, solve a CAPTCHA, or suppress an NSE
error. The operator selects the first date, leaves the calendar control
focused, and keeps the desired report checkboxes selected.

The helper presses `Tab` four times, activates the download, verifies that the
expected dated outer ZIP finishes in the Downloads directory, returns to the
calendar with `Shift+Tab`, and advances with the Right Arrow key. Calendar
advancement is date-driven: Friday to Monday sends three Right Arrow presses,
and month/year boundaries are crossed by the exact calendar-day difference.
Only Saturdays and Sundays are skipped. An NSE holiday or unavailable report
causes a timeout and stops the run so the UI cannot silently drift onto a
different date.

Preview a range without sending any keys:

```powershell
& .\scripts\nse_archive_keyboard_download.ps1 `
  -StartDate 2024-01-02 `
  -EndDate 2024-01-12 `
  -DryRun
```

Run it after selecting `2024-01-02` in the NSE calendar and focusing that
calendar control:

```powershell
& .\scripts\nse_archive_keyboard_download.ps1 `
  -StartDate 2024-01-02 `
  -EndDate 2024-01-12
```

The five-second countdown allows time to switch focus back to the browser.
Press `Escape` at any time to stop. Do not use the mouse or keyboard while the
helper is running. Browser "ask where to save" prompts must be disabled. Logs
are written under `C:\project\india-swing-data\download-automation`.

The focused window title must contain `Brave` by default. Use
`-ExpectedWindowTitlePattern Chrome` (or another explicit browser-family text)
when intentionally running it in a different browser. This guard prevents the
macro from sending keys into a terminal, editor, or chat window.

## NSE historical archive year stager

`stage_nse_historical_year.ps1` copies the year's downloaded outer ZIPs
(`Reports-Archives-Multiple-DDMMYYYY.zip`) into `source-archives/YYYY-MM-DD`
and extracts their entries into `staging/YYYY-MM-DD`, or copies an
unsupported/stale wrapper into
`quarantine/nse-historical-archive/YYYY-MM-DD` instead.

```powershell
& .\scripts\stage_nse_historical_year.ps1 `
  -Year 2022 `
  -DownloadDirectory C:\Users\kamal\Downloads `
  -DataRoot C:\project\india-swing-data
```

**Accepted entry-name profiles** (exact filenames only; any other entry set
is quarantined):

- One file: `sec_bhavdata_full_DDMMYYYY.csv` alone.
- Two files: adds `BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip`.
- Three files: adds `NSE_CM_security_DDMMYYYY.csv.gz`.
- Four files: adds `REG1_INDDDMMYY.csv`.

**One-file profile content guard.** A one-entry archive is staged only if
its CSV is read and validated in memory, before any `source-archives` or
`staging` session directory is created: non-empty, exactly one header row
whose trimmed field names include exactly one `DATE1` field, no duplicate
trimmed header fields, well-formed CSV quoting, consistent row widths, at
least one data row, and every `DATE1` value parses exactly as
`dd-MMM-yyyy` (invariant culture) and equals the outer ZIP's session date.
Any failure — mixed dates, a wrapper reporting the *preceding* session
(the real NSE holiday-wrapper pattern), missing/duplicate `DATE1` headers,
malformed rows or quoting, invalid date text, or empty data — routes the
whole archive to quarantine; it never partially populates
`source-archives` or `staging`. The two/three/four-file profiles are
unchanged and are not content-validated by this script.

**Raw downloads are never deleted or modified.** The script only copies
and extracts; `downloads_retained` in its JSON output is always `true`.

**Quarantine semantics.** Quarantined wrappers are copied, byte-verified,
to `quarantine/nse-historical-archive/YYYY-MM-DD/<name>-unsupported-or-stale-wrapper.zip`.
An identical retry is idempotent (hash-verified no-op); a pre-existing
quarantine file with different bytes at that path is a hard error and is
left unchanged.

**This script is not the validation authority.** It only gates what gets
copied into `source-archives`/`staging` from local Downloads. The
canonical Python importer (`import_nse_historical_range` /
`src/india_swing/market_data/nse_archive.py`) remains the final authority
on whether a staged session is actually imported.
