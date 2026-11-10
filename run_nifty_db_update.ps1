<#
  Windows-only NIFTY DB ingestion wrapper.

  Scheduled use: run at 18:00 IST after the Raspberry Pi has pushed the
  day's Output\YYYYMMDD\YYYYMMDDdata.csv file to GitHub.
  Manual use: .\run_nifty_db_update.ps1
  Manual historic retry: .\run_nifty_db_update.ps1 -TargetDate 20261109
#>

param(
    [string]$RepoDir = 'D:\nsetradingbot',
    [string]$PythonExe = 'D:\nsetradingbot\botenv\Scripts\python.exe',
    [string]$LogDir = 'D:\nsetradingbot\logs',
    [string]$TargetDate = ''
)

if (!(Test-Path -LiteralPath $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

$runTimestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = Join-Path $LogDir "nifty_db_update_$runTimestamp.log"

function Log([string]$Message) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $Message"
    $line | Tee-Object -FilePath $logFile -Append
}

function Exit-WithError([string]$Message, [int]$ExitCode = 1) {
    Log "ERROR: $Message"
    exit $ExitCode
}

try {
    Log "NIFTY DB update started. RepoDir=$RepoDir"

    if (!(Test-Path -LiteralPath $RepoDir -PathType Container)) {
        Exit-WithError "Repository directory not found: $RepoDir" 2
    }
    if (!(Test-Path -LiteralPath $PythonExe -PathType Leaf)) {
        Exit-WithError "Python executable not found: $PythonExe" 3
    }

    Set-Location -LiteralPath $RepoDir

    Log 'Pulling the Raspberry Pi backup from Git...'
    & git pull --rebase 2>&1 | Tee-Object -FilePath $logFile -Append
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "git pull --rebase failed with exit code $LASTEXITCODE" 4
    }

    if ([string]::IsNullOrWhiteSpace($TargetDate)) {
        $ist = [TimeZoneInfo]::FindSystemTimeZoneById('India Standard Time')
        $TargetDate = [TimeZoneInfo]::ConvertTimeFromUtc([datetime]::UtcNow, $ist).ToString('yyyyMMdd')
    }

    if ($TargetDate -notmatch '^\d{8}$') {
        Exit-WithError "TargetDate must be YYYYMMDD: $TargetDate" 5
    }
    try {
        [datetime]::ParseExact($TargetDate, 'yyyyMMdd', $null) | Out-Null
    } catch {
        Exit-WithError "TargetDate is not a valid calendar date: $TargetDate" 5
    }

    $outputDir = Join-Path $RepoDir (Join-Path 'Output' $TargetDate)
    $csvPath = Join-Path $outputDir ($TargetDate + 'data.csv')
    if (!(Test-Path -LiteralPath $csvPath -PathType Leaf)) {
        Exit-WithError "Today's NIFTY data file was not found: $csvPath" 6
    }
    if ((Get-Item -LiteralPath $csvPath).Length -eq 0) {
        Exit-WithError "Today's NIFTY data file is empty: $csvPath" 6
    }

    $header = Get-Content -LiteralPath $csvPath -TotalCount 1
    $requiredColumns = @('Datetime', 'Open', 'High', 'Low', 'Close', 'Volume')
    $missingColumns = $requiredColumns | Where-Object { $_ -notin ($header -split ',') }
    if ($missingColumns) {
        Exit-WithError "CSV is missing required columns: $($missingColumns -join ', ')" 7
    }

    Log "Validated input file: $csvPath"
    Log "Running database update for $TargetDate..."
    & $PythonExe 'Code\db\dbupdate.py' $TargetDate 2>&1 | Tee-Object -FilePath $logFile -Append
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "dbupdate.py failed with exit code $LASTEXITCODE" 8
    }

    Log "SUCCESS: Database update completed for $TargetDate."
    exit 0
} catch {
    Exit-WithError ($_.Exception.Message) 99
}
