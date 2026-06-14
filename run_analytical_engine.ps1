<#
    run_analytical_engine.ps1

    Purpose:
        Execute Analytical Engine once.

    Expected Schedule:
        09:30 AM IST

    Notes:
        Trade-date selection is handled by:
            AnalyticEngine/services/trade_date_resolver.py

        This script only launches the engine.
#>

param(
    [string]$RepoDir   = 'D:\nsetradingbot',
    [string]$PythonExe = 'D:\nsetradingbot\botenv\Scripts\python.exe',
    [string]$LogDir    = 'D:\nsetradingbot\logs'
)

if (!(Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

$ts = (Get-Date).ToString('yyyyMMdd_HHmmss')
$log = Join-Path $LogDir ("analytical_engine_" + $ts + ".log")

function Log($msg) {
    $line = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' - ' + $msg
    $line | Tee-Object -FilePath $log -Append
    Write-Host $line
}

try {

    Log "Analytical Engine Scheduler Started"

    Set-Location $RepoDir

    $scriptPath = Join-Path `
        $RepoDir `
        'AnalyticEngine\orchestrator\run_analysis.py'

    if (!(Test-Path $scriptPath)) {
        Log "ERROR: run_analysis.py not found"
        exit 1
    }

    Log "Executing Analytical Engine"

    & $PythonExe $scriptPath 2>&1 |
        Tee-Object -FilePath $log -Append

    $rc = $LASTEXITCODE

    if ($rc -ne 0) {
        Log "ERROR: Analytical Engine failed. RC=$rc"
        exit $rc
    }

    Log "Analytical Engine completed successfully"
    exit 0
}
catch {
    Log ("ERROR: " + ($_ | Out-String))
    exit 99
}