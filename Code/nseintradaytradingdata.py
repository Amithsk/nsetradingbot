# NSE Daily Bhavcopy Downloader
# Replicates user behaviour: homepage -> reports -> API -> download PreviousDay/Todays bhavcopy

import requests
import datetime
import time
from datetime import timedelta
import random
import json
from pathlib import Path
from urllib.parse import urljoin
from utils.nseholiday import nseholiday
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "Output" / "Intraday"
DEBUG_DIR = PROJECT_ROOT / "Output" / "Debug"

HOME_URL = "https://www.nseindia.com/"
REPORTS_URL = "https://www.nseindia.com/all-reports"
DAILY_API_URL = "https://www.nseindia.com/api/daily-reports?key=CM"

HEADERS_DICT = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com",
    "Origin": "https://www.nseindia.com",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}

def parse_api_response(resp) -> dict | None:
    """Parse NSE API response. Requests will auto-handle gzip/br if brotli installed."""
    try:
        data = resp.json()   # ✅ let requests handle decompression + parsing

        _save_debug({
            "status": resp.status_code,
            "headers": dict(resp.headers),
            "body": data      # save dict, not string
        })
        return data

    except Exception as e:
        print("Failed to parse API response:", e)
        try:
            _save_debug({
                "status": resp.status_code,
                "headers": dict(resp.headers),
                "body": resp.text[:2000]  # fallback string
            })
        except Exception:
            pass
        return None

def git_commit_changes(file_path: Path):
    """
    Commit and push changes to git.
    Assumes repo is already initialized and remote is configured.
    """
    trade_date = datetime.datetime.now().strftime("%d-%b-%Y")
    commit_message = f"Bhavcopy update {file_path.name} on {trade_date}"
    try:
        # Pull latest changes before pushing (to avoid rejection)
        subprocess.run(["git", "pull", "--rebase"], check=True)

        #Stage changes
        subprocess.run(["git", "add", "."], check=True)

        #Check if there are any  staged changes
        result = subprocess.run(["git", "diff", "--cached","--quiet"])
        if result.returncode == 0:
            print("No changes to commit.")
            return
        #Commit 
        subprocess.run(["git", "commit", "-m", commit_message], check=True)

        #Push changes
        subprocess.run(["git", "push"], check=True)
        print("Changes committed and pushed to git.")
    except subprocess.CalledProcessError as error:
        print("Git command failed:", error)

def establish_browser_session() -> requests.Session | None:
    """Open homepage + reports page to set cookies (simulate user)."""
    session_obj = requests.Session()
    session_obj.headers.update(HEADERS_DICT)

    try:
        response_home = session_obj.get(HOME_URL, timeout=10)
        print("Homepage:", response_home.status_code)
        time.sleep(random.uniform(2, 4))  # mimic human-like pause

        response_reports = session_obj.get(REPORTS_URL, timeout=10)
        print("Reports page:", response_reports.status_code)
        time.sleep(random.uniform(2, 4))
    except Exception as error:
        print("Session init failed:", error)
        return None

    return session_obj


def _save_file(session_obj: requests.Session, file_name: str, file_url: str, out_dir: Path = Path("Output/Intraday")) -> Path | None:
    """
    Download file_url (streamed) and save to out_dir/file_name.
    - Validates HTTP status
    - Streams to a .part file
    - Verifies ZIP magic bytes ('PK') before renaming to final file
    Returns Path on success, None on failure.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    final_path = out_dir / file_name
    tmp_path = out_dir / (file_name + ".part")

    # Make a safe copy of headers and set referer/origin for archive hosts
    headers = HEADERS_DICT.copy() if 'HEADERS' in globals() else {}
    headers.setdefault("Referer", HOME_URL if 'HOME_URL' in globals() else "https://www.nseindia.com/")
    headers.setdefault("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

    try:
        with session_obj.get(file_url, headers=headers, stream=True, timeout=30) as r:
            if r.status_code != 200:
                print(f"Failed to download {file_url} : HTTP {r.status_code}")
                # show small snippet for debugging (server often returns HTML/JSON on errors)
                try:
                    preview = r.text[:800]
                except Exception:
                    preview = "<no-preview-available>"
                print("Server response preview:", preview)
                return None

            # Stream first chunk to check magic bytes without loading whole content in memory
            it = r.iter_content(chunk_size=8192)
            first_chunk = next(it, b'')
            if not first_chunk:
                print("Empty response while downloading file.")
                return None

            # Quick Content-Type sanity check
            ct = r.headers.get("Content-Type", "").lower()
            if "html" in ct or "json" in ct or "text" in ct:
                # it's suspicious — show debug snippet and abort
                snippet = first_chunk[:500].decode(errors="replace")
                print(f"Download returned non-zip content (Content-Type: {ct}). Snippet:\n{snippet}")
                return None

            # If first_chunk doesn't start with ZIP magic, still write and check after complete stream
            with open(tmp_path, "wb") as fw:
                fw.write(first_chunk)
                for chunk in it:
                    if chunk:
                        fw.write(chunk)

            # Validate the downloaded file has ZIP signature (PK\x03\x04)
            with open(tmp_path, "rb") as fr:
                sig = fr.read(4)
            if not sig.startswith(b"PK"):
                print("Downloaded file does not have ZIP signature; removing .part file.")
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
                return None

            # All good -> rename
            tmp_path.replace(final_path)
            print("Saved:", final_path)
            return final_path

    except Exception as exc:
        print("Exception while downloading file:", exc)
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None

def download_bhavcopy_today(session_obj: requests.Session) -> Path | None:
    """
    Build today's PR<ddmmyy>.zip filename, query DAILY_API_URL to find the entry
    and pass the resulting file URL to _save_file() to perform the download.
    """
    today = datetime.datetime.now()
    file_name = f"PR{today.strftime('%d%m%y')}.zip"

    # warm cookies (optional but helpful)
    try:
        session_obj.get(HOME_URL, headers=HEADERS_DICT, timeout=10)
    except Exception:
        pass  # ignore warming failure; we'll still try the API

    # Query the index API (returns JSON with entries)
    try:
        resp = session_obj.get(DAILY_API_URL, headers=HEADERS_DICT, timeout=20)
    except Exception as e:
        print("Failed to call DAILY_API_URL:", e)
        return None

    if resp.status_code != 200:
        print(f"DAILY_API_URL returned HTTP {resp.status_code}")
        return None

    try:
        raw = resp.json()
        reports = raw.get("data", []) if isinstance(raw, dict) else []
    except Exception as e:
        print("DAILY_API_URL response isn't valid JSON:", e)
        return None

    # Find by fileActlName first, fallback to matching tradingDate
    report_entry = next((r for r in reports if r.get("fileActlName") == file_name), None)
    if not report_entry:
        today_str = today.strftime("%d-%b-%Y")
        report_entry = next((r for r in reports if r.get("tradingDate") == today_str), None)

    if not report_entry:
        print(f"No entry for {file_name} found in DAILY_API_URL response")
        return None

    # Build the download URL (filePath + fileActlName)
    file_path = report_entry.get("filePath") or ""
    file_actl = report_entry.get("fileActlName") or file_name
    file_url = file_path + file_actl

    print("Trying today file via API:", file_url)
    # download happens inside _save_file
    return _save_file(session_obj, file_name, file_url)

def download_bhavcopy_yesterday(session_obj: requests.Session) -> Path | None:
    """Download yesterday's bhavcopy (PRddmmyy.zip) using NSE Daily Reports API."""
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

    # Adjust for weekend
    if yesterday.weekday() == 5:  # Saturday → shift back to Friday
        file_date = yesterday - datetime.timedelta(days=1)
    elif yesterday.weekday() == 6:  # Sunday → shift back to Friday
        file_date = yesterday - datetime.timedelta(days=2)
    else:
        file_date = yesterday

    file_name = f"PR{file_date.strftime('%d%m%y')}.zip"


    # Warm cookies (optional but helpful)
    try:
        session_obj.get(HOME_URL, headers=HEADERS_DICT, timeout=10)
    except Exception:
        pass

    # Query API JSON
    try:
        resp = session_obj.get(DAILY_API_URL, headers=HEADERS_DICT, timeout=20)
    except Exception as e:
        print("Failed to call DAILY_API_URL:", e)
        return None

    if resp.status_code != 200:
        print(f"DAILY_API_URL returned HTTP {resp.status_code}")
        return None

    try:
        reports = resp.json().get("data", [])
    except Exception as e:
        print("DAILY_API_URL response isn't valid JSON:", e)
        return None

    # Try to match by file name first
    report_entry = next((r for r in reports if r.get("fileActlName") == file_name), None)
    if not report_entry:
        # Fallback: match by tradingDate (dd-MMM-YYYY)
        date_str = yesterday.strftime("%d-%b-%Y")
        report_entry = next((r for r in reports if r.get("tradingDate") == date_str), None)

    if not report_entry:
        print(f"No entry for {file_name} found in DAILY_API_URL response")
        return None

    # Build the download URL
    file_url = report_entry["filePath"] + report_entry["fileActlName"]

    print("Trying yesterday file via API:", file_url)
    return _save_file(session_obj, file_name, file_url)


def _save_debug(data_obj):
    """Save debug JSON to file."""
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(exist_ok=True)
    prefix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path_debug = DEBUG_DIR / f"daily_reports_debug_{prefix}.json"
    path_debug.write_text(json.dumps(data_obj, indent=2), encoding="utf-8")
    print(f"Debug JSON saved: {path_debug}")


def download_bhavcopy_master(session_obj: requests.Session) -> Path | None:
    """Master function: after 8 PM IST try today's file first, else yesterday."""
    now_time = datetime.datetime.now().time()
    cutoff_time = datetime.time(20, 0, 0)  # 8 PM

    if now_time >= cutoff_time:
        # Add random wait 0–15 min after cutoff
        wait_seconds = random.randint(0, 900)
        print(f"Waiting {wait_seconds} seconds before fetching today's bhavcopy...")
        time.sleep(wait_seconds)

        # Try today's file first
        file_path = download_bhavcopy_today(session_obj)
        if file_path:
            return file_path
        # Fallback to yesterday
        print("Today's file not available, falling back to PreviousDay API.")
        return download_bhavcopy_yesterday(session_obj)
    else:
        print("Before 8 PM → always use PreviousDay")
        # Before 8 PM → always use PreviousDay
        return download_bhavcopy_yesterday(session_obj)


def nse_is_open() -> bool:
    """Check if today is a trading day."""
    today = datetime.datetime.now()
    if today.weekday() in (5, 6):
        print("Weekend, market closed")
        return False
    if nseholiday(today.date()):
        print("Holiday, market closed")
        return False
    return True


if __name__ == "__main__":
    # Random startup delay (0–60 sec) to avoid looking like a bot
    startup_delay = random.randint(0, 60)
    print(f"Startup delay: {startup_delay} seconds")
    time.sleep(startup_delay)

    if not nse_is_open():
        print("NSE closed; skipping.")
    else:
        session_obj = establish_browser_session()
        if session_obj:
            file_path = download_bhavcopy_master(session_obj)
            if file_path:
                print("Download complete:", file_path)
                git_commit_changes(file_path)

            else:
                print("Bhavcopy not available yet. Check debug folder.")