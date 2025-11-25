"""
Polling script to be invoked by cron on Raspberry Pi.

- 19:00, 20:00, 21:00 → attempt TODAY only via run_download_flow("today")
- 07:00 → attempt YESTERDAY only via run_download_flow("yesterday")
- Poller reads local DEBUG_DIR/status.json written by nseintradaytradingdata
"""

import json
import time
from datetime import datetime, date, timedelta
from pathlib import Path

from nseintradaytradingdata import run_download_flow, DEBUG_DIR
from utils.nseholiday import nseholiday


# ------------------------------------------
# Helpers
# ------------------------------------------

def load_status():
    """Load status.json inside DEBUG_DIR / return dict."""
    status_file = Path(DEBUG_DIR) / "status.json"
    if not status_file.exists():
        return {
            "target_date": None,
            "state": "pending",
            "downloaded": None,
            "commit_sha": None,
            "note": "no_status_file",
            "ts": None
        }
    try:
        return json.loads(status_file.read_text())
    except Exception:
        return {
            "target_date": None,
            "state": "pending",
            "downloaded": None,
            "commit_sha": None,
            "note": "corrupt_status_file",
            "ts": None
        }


def is_evening_window():
    hr = datetime.now().hour
    return hr in (19, 20, 21)


def is_morning_window():
    return datetime.now().hour == 7


def today_str(d: date = None):
    d = d or datetime.now().date()
    return d.strftime("%Y-%m-%d")


# ------------------------------------------
# POLLER CORE LOGIC
# ------------------------------------------

def poll_bhavcopy():
    now = datetime.now()
    today = now.date()

    # Trading day check (we run poller only on trading days)
    if nseholiday(today):
        print(f"[SKIP] {today} is an NSE holiday.")
        return

    # Load last known status
    status = load_status()
    st_state = status.get("state")
    st_date = status.get("target_date")

    # ------------------------------------------
    # TERMINAL SUCCESS CONDITIONS
    # ------------------------------------------
    # If status.json shows success for today → no more work needed
    if st_date == today_str(today) and st_state in ("success", "downloaded", "committed"):
        print(f"[SKIP] Already success/committed for {today}. State={st_state}")
        return

    # ------------------------------------------
    # EVENING WINDOW: 19:00 → 21:59
    # ------------------------------------------
    if is_evening_window():
        print(f"[INFO] Evening polling {now.strftime('%H:%M')} — attempting TODAY only.")
        fp = run_download_flow(mode="today")
        if fp:
            print("[OK] Download flow succeeded for TODAY.")
        else:
            print("[INFO] Download attempt for TODAY returned None.")
        return

    # ------------------------------------------
    # MORNING FALLBACK: 07:00
    # ------------------------------------------
    if is_morning_window():
        print(f"[INFO] Morning fallback — attempting YESTERDAY.")
        fp = run_download_flow(mode="yesterday")
        if fp:
            print("[OK] Download flow succeeded for YESTERDAY.")
        else:
            print("[INFO] Download attempt for YESTERDAY returned None.")
        return

    # ------------------------------------------
    # Any other hour → ignore
    # ------------------------------------------
    print(f"[SKIP] Not a polling window at {now.strftime('%H:%M')}.")


# ------------------------------------------
# MAIN
# ------------------------------------------

if __name__ == "__main__":
    poll_bhavcopy()
