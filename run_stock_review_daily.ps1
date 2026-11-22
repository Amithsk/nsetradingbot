<#
  Windows Stock Review runner.

  It never checks out or pulls StockReview into the working tree.  Candle
  inputs are pinned to one commit from origin/StockReview and reports are
  generated with the existing Python command.
#>

param(
    [string]$RepoDir = 'D:\nsetradingbot',
    [string]$PythonExe = 'D:\nsetradingbot\botenv\Scripts\python.exe',
    [string]$LogDir = 'D:\logs\nsetradingbot',
    [string]$TargetDate = '',
    [string[]]$Symbols = @('BHARTIARTL', 'RELIANCE'),
    [int]$RetryMinutes = 30,
    [int]$RetryIntervalMinutes = 5
)

$ErrorActionPreference = 'Stop'

if (!(Test-Path -LiteralPath $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}
$logFile = Join-Path $LogDir 'stock_review_daily.log'

function Log([string]$Message) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $Message"
    Add-Content -LiteralPath $logFile -Value $line
    [Console]::WriteLine($line)
}

function Exit-WithError([string]$Message, [int]$ExitCode = 1) {
    Log "ERROR: $Message"
    exit $ExitCode
}

function Get-IstDateOnce {
    $ist = [TimeZoneInfo]::FindSystemTimeZoneById('India Standard Time')
    return [TimeZoneInfo]::ConvertTimeFromUtc([datetime]::UtcNow, $ist).ToString('yyyy-MM-dd')
}

function Resolve-StockRevision([string]$DateToken, [string[]]$Watchlist) {
    # Git writes normal fetch progress to stderr.  Do not let PowerShell's
    # global Stop preference turn that progress into an exception; trust the
    # native exit code below instead.
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $fetchOutput = & git fetch origin StockReview 2>&1
        $fetchExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    $fetchOutput | ForEach-Object { Log "git fetch: $_" }
    if ($fetchExit -ne 0) { throw "git fetch origin StockReview failed with exit code $fetchExit" }

    $sha = (& git rev-parse 'origin/StockReview^{commit}').Trim()
    if ($LASTEXITCODE -ne 0 -or !$sha) { throw 'could not resolve origin/StockReview to a commit' }

    foreach ($symbol in $Watchlist) {
        $path = "Output/$($DateToken.Replace('-',''))/$symbol.csv"
        & git cat-file -e "$sha`:$path" 2>$null
        if ($LASTEXITCODE -ne 0) { throw "missing Git input at $sha`: $path" }
    }
    return $sha
}

function Test-NiftyCoverage([string]$DateToken) {
    $python = @'
import sys
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text
sys.path.insert(0, sys.argv[2])
from Code.utils.nseintraday_db_utils import connect_db

trade_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
ist = ZoneInfo("Asia/Kolkata")
expected = [datetime.combine(trade_date, time(9, 15), tzinfo=ist) + timedelta(minutes=5*i) for i in range(72)]
engine = connect_db({"host": "localhost", "user": "root", "db": "nifty", "port": 3306})
try:
    with engine.connect() as conn:
        values = conn.execute(text("SELECT Date FROM nifty_prices WHERE DATE(Date)=:d ORDER BY Date"), {"d": trade_date}).scalars().all()
    actual = set()
    duplicates = set()
    for value in values:
        if value.tzinfo is None:
            value = value.replace(tzinfo=ist)
        value = value.astimezone(ist).replace(microsecond=0)
        if time(9, 15) <= value.time() <= time(15, 10):
            if value in actual: duplicates.add(value.isoformat())
            actual.add(value)
    missing = [value.isoformat() for value in expected if value not in actual]
    if missing or duplicates:
        print("NIFTY input incomplete: missing=" + str(missing[:3]) + " duplicates=" + str(sorted(duplicates)[:3]))
        raise SystemExit(1)
    print("NIFTY input ready: 72 aligned candles")
finally:
    engine.dispose()
'@
    $tempScript = [IO.Path]::GetTempFileName()
    try {
        Set-Content -LiteralPath $tempScript -Value $python -Encoding UTF8
        $previousPreference = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            $result = & $PythonExe $tempScript $DateToken $RepoDir 2>&1
            $exitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previousPreference
        }
        $result | ForEach-Object { Log "NIFTY check: $_" }
        return ($exitCode -eq 0)
    } finally {
        Remove-Item -LiteralPath $tempScript -Force -ErrorAction SilentlyContinue
    }
}

function Test-SavedReports([string]$DateToken, [string[]]$Watchlist) {
    $python = @'
import sys
from sqlalchemy import text
sys.path.insert(0, sys.argv[-1])
from Code.utils.nseintraday_db_utils import connect_db
trade_date, symbols = sys.argv[1], sys.argv[2:-1]
with connect_db().connect() as conn:
    for symbol in symbols:
        count = conn.execute(text("SELECT COUNT(*) FROM stock_review_daily WHERE exchange='NSE' AND symbol=:s AND trade_date=:d"), {"s": symbol, "d": trade_date}).scalar()
        print(f"{symbol}: {count} readable row(s)")
        if count != 1: raise SystemExit(1)
'@
    $tempScript = [IO.Path]::GetTempFileName()
    try {
        Set-Content -LiteralPath $tempScript -Value $python -Encoding UTF8
        $previousPreference = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            $result = & $PythonExe $tempScript $DateToken @Watchlist $RepoDir 2>&1
            $exitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previousPreference
        }
        $result | ForEach-Object { Log "Saved-report check: $_" }
        return ($exitCode -eq 0)
    } finally {
        Remove-Item -LiteralPath $tempScript -Force -ErrorAction SilentlyContinue
    }
}

try {
    if (!(Test-Path -LiteralPath $RepoDir -PathType Container)) { Exit-WithError "Repository not found: $RepoDir" 2 }
    if (!(Test-Path -LiteralPath $PythonExe -PathType Leaf)) { Exit-WithError "Python executable not found: $PythonExe" 3 }
    if (!$Symbols -or $Symbols.Count -eq 0) { Exit-WithError 'At least one report symbol is required' 4 }
    $Symbols = @($Symbols | ForEach-Object { $_.Trim().ToUpper() } | Select-Object -Unique)
    Set-Location -LiteralPath $RepoDir

    # Capture the implicit target exactly once so a midnight rollover cannot
    # mix the Git date, NIFTY query date, and report date.
    if ([string]::IsNullOrWhiteSpace($TargetDate)) { $TargetDate = Get-IstDateOnce }
    try { [datetime]::ParseExact($TargetDate, 'yyyy-MM-dd', $null) | Out-Null }
    catch { Exit-WithError "TargetDate must be YYYY-MM-DD: $TargetDate" 4 }
    Log "Stock Review started. target_date=$TargetDate symbols=$($Symbols -join ',')"

    $attempts = [math]::Floor($RetryMinutes / $RetryIntervalMinutes) + 1
    $sha = $null
    for ($attempt = 1; $attempt -le $attempts; $attempt++) {
        try {
            $candidate = Resolve-StockRevision $TargetDate $Symbols
            $niftyReady = Test-NiftyCoverage $TargetDate
            if ($niftyReady) { $sha = $candidate; break }
        } catch {
            Log "Input attempt $attempt/$attempts pending: $($_.Exception.Message)"
        }
        if ($attempt -lt $attempts) {
            Log "Inputs pending; retrying in $RetryIntervalMinutes minute(s)."
            Start-Sleep -Seconds ($RetryIntervalMinutes * 60)
        }
    }
    if (!$sha) { Exit-WithError "Required inputs remain unavailable for $TargetDate after $RetryMinutes minutes" 5 }
    Log "Inputs ready at Git revision $sha"

    $failed = $false
    foreach ($symbol in $Symbols) {
        $output = & $PythonExe -m stock_review --symbol $symbol --trade-date $TargetDate --git-revision $sha --save 2>&1
        $output | ForEach-Object { Log "${symbol}: $_" }
        if ($LASTEXITCODE -ne 0) {
            Log "ERROR: $symbol report generation failed with exit code $LASTEXITCODE"
            $failed = $true
            continue
        }
        $lastJson = $output | Where-Object { $_ -match '"saved"' } | Select-Object -Last 1
        if ($lastJson -match '"saved"\s*:\s*true') { Log "${symbol}: report saved" }
        elseif ($lastJson -match '"saved"\s*:\s*false') { Log "${symbol}: existing report reused" }
        else { Log "${symbol}: report command succeeded (save state not parsed)" }
    }
    if ($failed) { Exit-WithError 'One or more stock reports failed' 6 }
    if (!(Test-SavedReports $TargetDate $Symbols)) { Exit-WithError 'Saved-report verification failed' 7 }
    Log "SUCCESS: Stock Review completed. revision=$sha target_date=$TargetDate"
    exit 0
} catch {
    Exit-WithError $_.Exception.Message 99
}
