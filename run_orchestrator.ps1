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
    [string]$LogDir     = "D:\nsebot\logs"
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

    # 2) Read status.json
    if (!(Test-Path $StatusFile)) {
        Log "Status file not found: $StatusFile -- exiting."
        exit 3
    }

    try {
        $statusJson = Get-Content $StatusFile -Raw | ConvertFrom-Json
    } catch {
        Log "ERROR: Failed to parse status.json: $_"
        exit 4
    }

    $state = ($statusJson.state) -as [string]
    $target_date = ($statusJson.target_date) -as [string]
    if (-not $target_date) {
        Log "status.json missing target_date — exiting."
        exit 5
    }

    $today = (Get-Date).ToString("yyyy-MM-dd")
    Log "Status read: state=$state target_date=$target_date today=$today"

    $successStates = @("committed","success","downloaded")
    if ($successStates -contains ($state.ToLower())) {
        if ($target_date -eq $today) {
            Log "Status indicates today's data present (state=$state). Proceeding to DB flows."
        } else {
            Log "Status indicates success but target_date ($target_date) != today ($today). Exiting."
            exit 0
        }
    } else {
        Log "Status not ready for ingestion (state=$state). Exiting."
        exit 0
    }

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
