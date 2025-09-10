# NSE Daily Bhavcopy Downloader
# Replicates user behaviour: homepage -> reports -> API -> download PreviousDay bhavcopy

import requests
import datetime
import time
import random
import json
from pathlib import Path
from urllib.parse import urljoin
from utils.nseholiday import nseholiday

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


def save_daily_reports_debug(session_obj: requests.Session) -> None:
    """Save full JSON for debugging if bhavcopy not found."""
    try:
        response_api = session_obj.get(DAILY_API_URL, timeout=20)
        if response_api.status_code != 200:
            print("API debug failed:", response_api.status_code)
            return

        reports_data = response_api.json()
        prefix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        debug_dir = Path("debug")
        debug_dir.mkdir(exist_ok=True)

        full_path = debug_dir / f"daily_reports_{prefix}.json"
        full_path.write_text(json.dumps(reports_data, indent=2), encoding="utf-8")

        print(f"Debug saved: {full_path}")
    except Exception as error:
        print("Debug error:", error)


def download_bhavcopy_daily(session_obj: requests.Session) -> Path | None:
    """Download previous day's PRddmmyy.zip bhavcopy file from API (PreviousDay section)."""
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
            save_daily_reports_debug(session_obj)
            return None

        file_name = bhavcopy_entry["fileActlName"]
        file_url = urljoin(bhavcopy_entry["filePath"], file_name)

        return _save_file(session_obj, file_name, file_url)

    except Exception as error:
        print("Error fetching bhavcopy:", error)
        return None


def _save_file(session_obj: requests.Session, file_name: str, file_url: str) -> Path | None:
    """Download and save file to bhavcopy/ folder."""
    save_dir = Path("bhavcopy")
    save_dir.mkdir(exist_ok=True)
    save_path = save_dir / file_name

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
    if not nse_is_open():
        print("NSE closed; skipping.")
    else:
        session_obj = establish_browser_session()
        if session_obj:
            bhavcopy_path = download_bhavcopy_daily(session_obj)
            if bhavcopy_path:
                print("Download complete:", bhavcopy_path)
            else:
                print("Bhavcopy not available yet. Check debug folder.")
