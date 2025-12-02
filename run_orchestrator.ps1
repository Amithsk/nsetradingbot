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
    [string]$RepoDir    = 'D:\nsetradingbot',
    [string]$PythonExe  = 'D:\nsetradingbot\botenv\Scripts\python.exe',  # should point to python.exe
    [string]$StatusFile = 'D:\nsetradingbot\debug\status.json',
    [string]$LogDir     = 'D:\nsetradingbot\logs'
)

# ensure logs dir
if (!(Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }
$ts = (Get-Date).ToString('yyyyMMdd_HHmmss')
$log = Join-Path $LogDir ('orchestrator_' + $ts + '.log')

function Log($msg) {
    $line = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' - ' + $msg
    $line | Tee-Object -FilePath $log -Append
    Write-Host $line
}

# helper to run a python script and check rc
function Run-PythonScript($pythonExe, $scriptPath, [string[]] $args) {
    Log ('Executing Python: ' + $pythonExe + ' ' + $scriptPath + ' ' + ($args -join ' '))
    try {
        & $pythonExe $scriptPath @args 2>&1 | Tee-Object -FilePath $log -Append
        $rc = $LASTEXITCODE
    } catch {
        $err = $_ | Out-String
        Log ('ERROR: Failed to start Python for ' + $scriptPath + ' : ' + $err)
        return @{ success = $false; rc = 999 }
    }
    if ($rc -ne 0) {
        Log ('ERROR: Script exited with code ' + $rc + ' : ' + $scriptPath)
        return @{ success = $false; rc = $rc }
    } else {
        Log ('OK: Completed ' + $scriptPath + ' (rc=0)')
        return @{ success = $true; rc = 0 }
    }
}

try {
    Log ('Orchestrator start. RepoDir=' + $RepoDir)

    if (!(Test-Path $RepoDir)) {
        Log ('ERROR: RepoDir not found: ' + $RepoDir)
        exit 2
    }

    Set-Location $RepoDir

    # 1) git pull (safe, non-fatal)
    Log 'Running git pull...'
    try {
        $gitOut = & git pull 2>&1
        $gitRC = $LASTEXITCODE
        $gitOut | Tee-Object -FilePath $log -Append
        if ($gitRC -ne 0) {
            Log ('WARNING: git pull returned exit ' + $gitRC + ' - continuing (check logs)')
        } else {
            Log 'git pull succeeded.'
        }
    } catch {
        $errStr = $_ | Out-String
        Log ('ERROR: git pull execution failed: ' + $errStr)
        # continue; repository may already be up-to-date locally
    }

    # 2) Ensure status file exists
    if (!(Test-Path $StatusFile)) {
        Log ('Status file not found: ' + $StatusFile + ' -- exiting.')
        exit 3
    }

    # 3) Read status.json robustly (retry loop)
    $statusJson = $null
    $rawContent = ''
    $maxRetries = 5

    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        try {
            $rawContent = Get-Content $StatusFile -Raw -ErrorAction Stop
            $rawPreview = $rawContent
            if ($rawPreview.Length -gt 2000) { $rawPreview = $rawPreview.Substring(0,2000) + '...(truncated)' }
            Log ('DEBUG: Raw status.json content (attempt ' + $attempt + '): ' + "`n" + $rawPreview)

            if ($rawContent.Trim().Length -gt 0) {
                try {
                    $statusJson = $rawContent | ConvertFrom-Json -ErrorAction Stop
                    Log ('DEBUG: status.json parsed successfully on attempt ' + $attempt + '.')
                    break
                } catch {
                    $errStr = $_ | Out-String
                    Log ('WARN: ConvertFrom-Json failed on attempt ' + $attempt + ': ' + $errStr)
                }
            } else {
                Log ('WARN: status.json empty on attempt ' + $attempt + '.')
            }
        } catch {
            $errStr = $_ | Out-String
            Log ('WARN: Failed to read status.json on attempt ' + $attempt + ': ' + $errStr)
        }
        Start-Sleep -Seconds (Get-Random -Minimum 1 -Maximum 2)
    }

    if ($statusJson -eq $null) {
        Log ('ERROR: Unable to load valid status.json after ' + $maxRetries + ' attempts. Last raw content:')
        $rawPreview = $rawContent
        if ($rawPreview.Length -gt 4000) { $rawPreview = $rawPreview.Substring(0,4000) + '...(truncated)' }
        Log $rawPreview
        exit 4
    }

    # 4) Extract fields safely
    $state = $null
    $target_date = $null
    $downloaded = $null

    if ($statusJson.PSObject.Properties.Match('state'))      { $state      = [string]$statusJson.state }
    if ($statusJson.PSObject.Properties.Match('target_date')){ $target_date= [string]$statusJson.target_date }
    if ($statusJson.PSObject.Properties.Match('downloaded')) { $downloaded = [string]$statusJson.downloaded }


    if (-not $target_date -or $target_date.Trim().Length -eq 0) {
        Log 'status.json missing or empty target_date - see content below:'
        $rawPreview = $rawContent
        if ($rawPreview.Length -gt 4000) { $rawPreview = $rawPreview.Substring(0,4000) + '...(truncated)' }
        Log $rawPreview
        exit 5
    }

    Log ('Status read (extracted): state=' + ($state -or '<NULL>') + ' target_date=' + $target_date + ' downloaded=' + ($downloaded -or '<NULL>'))

    # 5) Decision: proceed only if status indicates today's data present
    $today = (Get-Date).ToString('yyyy-MM-dd')
    $successStates = @('committed','success','downloaded')

    if ($successStates -contains ($state -as [string])) {
        if ($target_date -eq $today) {
            Log ('Status indicates today''s data present (state=' + $state + '). Proceeding to DB flows.')
        } else {
            Log ('Status indicates success but target_date (' + $target_date + ') != today (' + $today + '). Exiting.')
            exit 0
        }
    } else {
        Log ('Status not ready for ingestion (state=' + ($state -or '<NULL>') + '). Exiting.')
        exit 0
    }

    # 6) Resolve ZIP path to pass to trade DB updater
    $zipPath = $null
    if ($downloaded -and $downloaded.Trim().Length -gt 0) {
        $zipPath = Join-Path $RepoDir (Join-Path 'Output\Intraday' $downloaded)
    } else {
        # construct PRddmmyy.zip from target_date
        try {
            $dt = [datetime]::ParseExact($target_date, 'yyyy-MM-dd', $null)
            $fname = 'PR' + $dt.ToString('ddMMyy') + '.zip'
            $zipPath = Join-Path $RepoDir (Join-Path 'Output\Intraday' $fname)
        } catch {
            $err = $_ | Out-String
            Log ('ERROR: Failed to parse target_date to construct zip filename: ' + $err)
            exit 6
        }
    }

    Log ('Resolved zip path: ' + $zipPath)

    if (!(Test-Path $zipPath)) {
        Log ('ERROR: Bhavcopy zip not found: ' + $zipPath + ' - cannot proceed.')
        exit 10
    }

    # 7) Validate python executable path
    $pythonExeResolved = $null
    if (Test-Path $PythonExe -PathType Leaf) {
        $pythonExeResolved = $PythonExe
    } elseif (Test-Path (Join-Path $PythonExe 'python.exe')) {
        $pythonExeResolved = Join-Path $PythonExe 'python.exe'
    } else {
        # fall back to system 'python' on PATH
        $which = & where.exe python 2>$null
        if ($which) { $pythonExeResolved = $which[0] } else { $pythonExeResolved = $null }
    }

    if (-not $pythonExeResolved) {
        Log ('ERROR: python executable not found. Checked: ' + $PythonExe + ' and PATH fallback. Aborting.')
        exit 11
    }

    Log ('Using python executable: ' + $pythonExeResolved)

    $markerDir = Join-Path $RepoDir 'Output\Intraday'
    $markerFile = Join-Path $markerDir ".ingested_$target_date"

    if (Test-Path $markerFile) {
        Log "Marker present ($markerFile) - ingestion already completed previously. Exiting."
    exit 0
    }

    # 8) Run DB scripts in order. Fail fast.
    $tradeScript  = Join-Path $RepoDir 'Code\db\nseintradaytradedbupdate.py'
    $signalScript = Join-Path $RepoDir 'Code\db\nseintradaytradingsignaldbupdate.py'
    $evalScript   = Join-Path $RepoDir 'Code\db\nseintradaylabelevaluationdbupdate.py'
 

    # sanity check scripts exist
    foreach ($p in @($tradeScript, $signalScript, $evalScript)) {
        if (!(Test-Path $p)) {
            Log ('ERROR: Required script not found: ' + $p)
            exit 12
        }
    }

    # 8.1 Trade DB update (pass zip path)
    $res = Run-PythonScript -pythonExe $pythonExeResolved -scriptPath $tradeScript -args @($zipPath)
    if (-not $res.success) {
        Log ('ERROR: Trade DB update failed (rc=' + $res.rc + '). Aborting.')
        exit 7
    }

    # 8.2 Signal generation (explicit date)
    $res = Run-PythonScript -pythonExe $pythonExeResolved -scriptPath $signalScript -args @('--date', $target_date)
    if (-not $res.success) {
        Log ('ERROR: Signal generation failed (rc=' + $res.rc + '). Aborting.')
        exit 8
    }

    # 8.3 Label / evaluation (start=end=target_date)
    $res = Run-PythonScript -pythonExe $pythonExeResolved -scriptPath $evalScript -args @('--start-date', $target_date, '--end-date', $target_date)
    if (-not $res.success) {
        Log ('ERROR: Evaluation failed (rc=' + $res.rc + '). Aborting.')
        exit 9
    }

    Log 'All DB flows completed successfully.'
    # create marker so subsequent scheduled runs skip work
    try {
        New-Item -Path $markerFile -ItemType File -Force | Out-Null
        Log ("Created ingest marker: " + $markerFile)
    } catch {
        $err = $_ | Out-String
        Log ("WARN: Failed to create marker file: " + $err)
    }
    exit 0

} catch {
    $err = $_ | Out-String
    Log ('Unhandled exception: ' + $err)
    exit 99
}
