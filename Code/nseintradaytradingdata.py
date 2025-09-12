# NSE Daily Bhavcopy Downloader
# Replicates user behaviour: homepage -> reports -> API -> download PreviousDay/Todays bhavcopy

import requests
import datetime
import time
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/all-reports",
    "Origin": "https://www.nseindia.com",
    "Connection": "keep-alive"
}

def git_commit_changes(file_path: Path):
    """
    Commit and push changes to git.
    Assumes repo is already initialized and remote is configured.
    """
    trade_date = datetime.datetime.now().strftime("%d-%b-%Y")
    commit_message = f"Bhavcopy update {file_path.name} on {trade_date}"
    try:
        #Stage changes
        subprocess.run(["git", "add", "."], check=True)

        #Check if there are any  staged changes
        result = subprocess.run(["git", "diff", "--cached","--quiet"])
        if result.returncode == 0:
            print("No changes to commit.")
            return
        #Commit and push
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
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


def _save_file(session_obj: requests.Session, file_name: str, file_url: str) -> Path | None:

    """Download and save file to bhavcopy/ folder."""
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = OUTPUT_DIR / file_name

    try:
        response_file = session_obj.get(file_url, stream=True, timeout=30)
        if response_file.status_code == 200:
            with open(save_path, "wb") as file_handle:
                for chunk in response_file.iter_content(chunk_size=8192):
                    if chunk:
                        file_handle.write(chunk)
            print(f"Saved: {save_path}")
            return save_path
        else:
            print("Download failed:", response_file.status_code)
            return None
    except Exception as error:
        print("File download error:", error)
        return None


def download_bhavcopy_today(session_obj: requests.Session) -> Path | None:
    """Try to download today's bhavcopy (T) via archives URL."""
    today = datetime.datetime.now()
    file_name = f"cm{today.strftime('%d%b%Y').upper()}bhav.csv.zip"
    file_url = (
        "https://archives.nseindia.com/content/historical/EQUITIES/"
        f"{today.strftime('%Y')}/{today.strftime('%b').upper()}/{file_name}"
    )
    print("Trying today file:", file_url)
    return _save_file(session_obj, file_name, file_url)


def download_bhavcopy_yesterday(session_obj: requests.Session) -> Path | None:
    """Download yesterday's bhavcopy from API PreviousDay section."""
    try:
        response_api = session_obj.get(DAILY_API_URL, timeout=20)
        if response_api.status_code != 200:
            print("Daily API failed:", response_api.status_code)
            return None

        reports_data = response_api.json()
        previous_day_reports = reports_data.get("PreviousDay", [])

        bhavcopy_entry = None
        for item in previous_day_reports:
            if "BHAVCOPY-PR-ZIP" in (item.get("fileKey") or ""):
                bhavcopy_entry = item
                break

        if not bhavcopy_entry:
            print("Bhavcopy entry not found in PreviousDay")
            _save_debug(reports_data)
            return None

        file_name = bhavcopy_entry["fileActlName"]
        file_url = urljoin(bhavcopy_entry["filePath"], file_name)
        return _save_file(session_obj, file_name, file_url)

    except Exception as error:
        print("Error fetching bhavcopy (yesterday):", error)
        return None


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