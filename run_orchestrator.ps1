<#
  run_orchestrator.ps1
  Desktop orchestrator wrapper for 19:00/20:00/21:00 runs.

  Behavior:
   - cd to $RepoDir
   - git pull
   - read $StatusFile (D:\nsetradingbot\debug\status.json)
   - if state in (committed, success, downloaded) and target_date == today -> run DB scripts sequentially
   - otherwise exit
#>

param(
    [string]$RepoDir    = "D:\nsetradingbot",
    [string]$PythonExe  = "D:\nsetradingbot\botenv\Scripts\python.exe",
    [string]$StatusFile = "D:\nsetradingbot\debug\status.json",
    [string]$LogDir     = "D:\nsetradingbot\logs"
)

# --- helper to log lines both to console and file ---
if (!(Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$log = Join-Path $LogDir "orchestrator_$ts.log"
function Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $msg"
    $line | Tee-Object -FilePath $log -Append
    Write-Host $line
}

try {
    Log "Orchestrator start. RepoDir=$RepoDir"

    if (!(Test-Path $RepoDir)) {
        Log "ERROR: RepoDir not found: $RepoDir"
        exit 2
    }

    Set-Location $RepoDir

    # 1) Pull latest changes
    Log "Running git pull..."
    $gitOut = & git pull 2>&1
    $gitRC = $LASTEXITCODE
    $gitOut | Tee-Object -FilePath $log -Append
    if ($gitRC -ne 0) {
        Log "WARNING: git pull returned exit $gitRC — continuing (check logs)"
    } else {
        Log "git pull succeeded."
    }

        # 2) Read status.json (robust with retries + logging of raw content)
    if (!(Test-Path $StatusFile)) {
        Log "Status file not found: $StatusFile -- exiting."
        exit 3
    }

    $statusJson = $null
    $rawContent = ""
    $maxRetries = 5

    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        try {
            $rawContent = Get-Content $StatusFile -Raw -ErrorAction Stop
            # Log raw content (trim to reasonable length)
            $rawPreview = $rawContent
            if ($rawPreview.Length -gt 2000) { $rawPreview = $rawPreview.Substring(0,2000) + "...(truncated)" }
            Log "DEBUG: Raw status.json content (attempt $attempt): `n$rawPreview"
            if ($rawContent.Trim().Length -gt 0) {
                try {
                    $statusJson = $rawContent | ConvertFrom-Json -ErrorAction Stop
                    Log "DEBUG: status.json parsed successfully on attempt $attempt."
                    break
                } catch {
                    Log "WARN: ConvertFrom-Json failed on attempt $attempt: $_"
                }
            } else {
                Log "WARN: status.json empty on attempt $attempt."
            }
        } catch {
            Log "WARN: Failed to read status.json on attempt $attempt: $_"
        }
        Start-Sleep -Seconds (Get-Random -Minimum 1 -Maximum 2)
    }

    if ($statusJson -eq $null) {
        Log "ERROR: Unable to load valid status.json after $maxRetries attempts. Last raw content:"
        $rawPreview = $rawContent
        if ($rawPreview.Length -gt 4000) { $rawPreview = $rawPreview.Substring(0,4000) + "...(truncated)" }
        Log $rawPreview
        exit 4
    }

    # Safely extract fields
    $state = $null
    $target_date = $null
    if ($statusJson.PSObject.Properties.Match('state')) { $state = ($statusJson.state) -as [string] }
    if ($statusJson.PSObject.Properties.Match('target_date')) { $target_date = ($statusJson.target_date) -as [string] }

    if (-not $target_date -or $target_date.Trim().Length -eq 0) {
        Log "status.json missing or empty target_date — see content below:"
        $rawPreview = $rawContent
        if ($rawPreview.Length -gt 4000) { $rawPreview = $rawPreview.Substring(0,4000) + "...(truncated)" }
        Log $rawPreview
        exit 5
    }

    # Log the extracted values we will use
    Log "Status read (extracted): state=$($state -or '<NULL>') target_date=$target_date"

    # 3) DB scripts to run (in order)
    $scripts = @(
        "Code\db\nseintradaytradedbupdate.py",
        "Code\db\nseintradaytradingsignaldbupdate.py",
        "Code\db\nnseintradaylabelevaluationdbupdate.py"
    )

    foreach ($s in $scripts) {
        $scriptPath = Join-Path $RepoDir $s
        if (!(Test-Path $scriptPath)) {
            Log "ERROR: Script not found: $scriptPath. Aborting."
            exit 6
        }

        Log "Executing: $PythonExe `"$scriptPath`""
        & $PythonExe $scriptPath *>&1 | Tee-Object -FilePath $log -Append
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0) {
            Log "ERROR: Script failed: $scriptPath exit=$exitCode. Aborting remaining steps."
            exit 7
        }
        Log "OK: Completed $scriptPath"
    }

    Log "All DB flows completed successfully."
    exit 0
}
catch {
    Log "Unhandled exception: $_"
    exit 99
}
