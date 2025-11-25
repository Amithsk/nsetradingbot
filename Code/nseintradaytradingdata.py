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
OUTPUT_DIR     = PROJECT_ROOT / "Output" / "Intraday"
DEBUG_DIR      = PROJECT_ROOT / "debug"
STATUS_FILE    = DEBUG_DIR / "status.json"

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

def adjust_for_weekend(date: datetime.datetime) -> datetime.datetime:
    """Shift Sat/Sun back to Friday."""
    if date.weekday() == 5:  # Saturday
        return date - timedelta(days=1)
    if date.weekday() == 6:  # Sunday
        return date - timedelta(days=2)
    return date

#---------------------Debug  update function starts here-----------------------   
def _save_debug(data_obj):
    """Save debug JSON to file."""
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(exist_ok=True)
    prefix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path_debug = DEBUG_DIR / f"daily_reports_debug_{prefix}.json"
    path_debug.write_text(json.dumps(data_obj, indent=2), encoding="utf-8")
    print(f"Debug JSON saved: {path_debug}")
#---------------------Debug  update function ends here-----------------------   


#---------------------Status update function starts here-----------------------
def _save_status(status_obj: dict):
    """
    Writes the authoritative status.json inside DEBUG_DIR.
    This file is always overwritten (hybrid model).
    Poller/orchestrator will read this file directly from the repo.

    status_obj structure example:
    {
        "target_date": "2025-11-22",
        "state": "success" | "pending" | "failed" | "holiday",
        "downloaded": "PR221125.zip" | None,
        "downloaded_at": "2025-11-22T19:03:12+05:30",
        "source": "download_bhavcopy_today",
        "note": "...",
        "debug_file": "daily_reports_debug_20251122_190312.json",
        "run_id": "20251122_190312"
    }
    """
    try:
        STATUS_FILE = DEBUG_DIR / "status.json"
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)

        # Add timestamp to the status object
        status_obj["ts"] = datetime.datetime.now().isoformat()

        # Atomic write: write to temp → rename
        tmp = STATUS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(status_obj, indent=2), encoding="utf-8")
        tmp.replace(STATUS_FILE)

        print(f"Status JSON updated at: {STATUS_FILE}")

    except Exception as e:
        print("ERROR writing status.json:", e)
#---------------------Status update function ends here-----------------------

def collect_all_reports(data: dict) -> list[dict]:
    """Merge CurrentDay, PreviousDay, FutureDay arrays from API JSON."""
    reports = []
    for key in ("CurrentDay", "PreviousDay", "FutureDay"):
        if key in data and isinstance(data[key], list):
            reports.extend(data[key])
    return reports

def parse_api_response(resp) -> dict | None:
    """Parse NSE API response. Requests will auto-handle gzip/br if brotli installed."""
    try:
        data = resp.json()   # let requests handle decompression + parsing

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
    Commit & push artifact and ensure final status.json in repo contains the artifact commit SHA.

    Workflow (Option B):
      1) Write status.json with state="downloaded" (local).
      2) git add artifact + status.json (+ recent debug files), git commit -> commit A
      3) fetch + rebase on origin/main (to avoid push rejection)
      4) git push  -> commit A lands on remote
      5) update status.json with state="committed" + commit_sha
      6) git commit + git push -> commit B

    Returns:
      - commit_a_sha on success (string) or None on failure.
    """
    debug_record = {
        "action": "git_commit_changes",
        "file": str(file_path) if file_path is not None else None,
        "ts": datetime.datetime.now().isoformat(),
        "result": None,
        "error": None,
        "commit_a_sha": None,
        "commit_b_sha": None,
    }

    try:
        file_path = Path(file_path) if file_path is not None else None

        # -------------------------------------------------
        # Build list of paths to stage for Commit A
        # -------------------------------------------------
        to_add = []

        if file_path and file_path.exists():
            to_add.append(str(file_path))

        # OUTPUT_DIR (optional)
        try:
            if 'OUTPUT_DIR' in globals() and OUTPUT_DIR.exists():
                to_add.append(str(OUTPUT_DIR))
        except Exception:
            pass

        # STATUS_FILE
        try:
            if 'STATUS_FILE' in globals():
                status_path = STATUS_FILE
            else:
                status_path = DEBUG_DIR / "status.json"
        except Exception:
            status_path = DEBUG_DIR / "status.json"

        # -------------------------
        # Write state = downloaded
        # -------------------------
        try:
            target_date = None
            if file_path and file_path.name.startswith("PR") and file_path.name.endswith(".zip"):
                try:
                    fname = file_path.name
                    dd = fname[2:4]; mm = fname[4:6]; yy = fname[6:8]
                    target_date = datetime.date(int("20"+yy), int(mm), int(dd)).isoformat()
                except Exception:
                    target_date = None

            _save_status({
                "target_date": target_date,
                "state": "downloaded",
                "downloaded": file_path.name if file_path else None,
                "downloaded_at": datetime.datetime.now().isoformat(),
                "source": "git_commit_changes",
                "note": "artifact downloaded locally; about to commit"
            })
        except Exception:
            pass

        # Add status.json if present
        try:
            if status_path.exists():
                to_add.append(str(status_path))
        except Exception:
            pass

        # Include recent debug logs (optional)
        try:
            if DEBUG_DIR.exists():
                recent = sorted(DEBUG_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:3]
                for df in recent:
                    to_add.append(str(df))
        except Exception:
            pass

        # If nothing to commit
        if not to_add:
            debug_record["result"] = "no_changes_to_commit"
            _save_debug(debug_record)
            try:
                _save_status({
                    "target_date": None,
                    "state": "no_changes_to_commit",
                    "downloaded": file_path.name if file_path else None,
                    "source": "git_commit_changes"
                })
            except Exception:
                pass
            print("No files to commit.")
            return None

        # ---------------------------------
        # Commit A (local commit)
        # ---------------------------------
        for p in to_add:
            subprocess.run(["git", "add", p], check=True)

        if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
            debug_record["result"] = "no_changes_to_commit"
            _save_debug(debug_record)
            _save_status({
                "target_date": target_date,
                "state": "no_changes_to_commit",
                "source": "git_commit_changes"
            })
            print("No staged changes to commit.")
            return None

        commit_msg_a = f"Bhavcopy artifact {file_path.name if file_path else ''} - downloaded on {datetime.datetime.now().strftime('%d-%b-%Y')}"
        subprocess.run(["git", "commit", "-m", commit_msg_a], check=True)
        print("Commit A created locally.")

        # ----------------------------------------------------------
        # NEW FIX: Fetch + rebase BEFORE pushing (avoids push reject)
        # ----------------------------------------------------------
        try:
            subprocess.run(["git", "fetch", "origin"], check=True)

            rebase_proc = subprocess.run(["git", "rebase", "origin/main"])
            if rebase_proc.returncode != 0:
                subprocess.run(["git", "rebase", "--abort"], check=False)

                debug_record["result"] = "rebase_failed"
                debug_record["error"] = "rebase failed; manual conflict resolution required"
                _save_debug(debug_record)

                _save_status({
                    "target_date": target_date,
                    "state": "failed",
                    "downloaded": file_path.name if file_path else None,
                    "source": "git_commit_changes",
                    "error": "git rebase failed - manual intervention required"
                })

                print("REBASE FAILED — manual fix required. Push aborted.")
                return None

            print("Rebase succeeded — safe to push.")

        except subprocess.CalledProcessError as err:
            debug_record["result"] = "fetch_failed"
            debug_record["error"] = str(err)
            _save_debug(debug_record)
            _save_status({
                "target_date": target_date,
                "state": "failed",
                "downloaded": file_path.name if file_path else None,
                "source": "git_commit_changes",
                "error": f"git fetch failed: {err}"
            })
            print("FETCH FAILED — aborting.")
            return None

        # -----------------------
        # Push Commit A
        # -----------------------
        subprocess.run(["git", "push"], check=True)
        print("Commit A pushed successfully.")

        # Get commit A SHA
        try:
            commit_a_sha = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True, text=True, check=True
            ).stdout.strip()
        except Exception:
            commit_a_sha = None

        debug_record["result"] = "committed_and_pushed_artifact"
        debug_record["commit_a_sha"] = commit_a_sha
        _save_debug(debug_record)

        # -------------------------------------------
        # Commit B → update status.json with SHA
        # -------------------------------------------
        _save_status({
            "target_date": target_date,
            "state": "committed",
            "downloaded": file_path.name if file_path else None,
            "source": "git_commit_changes",
            "commit_sha": commit_a_sha,
            "note": "artifact commit pushed"
        })

        subprocess.run(["git", "add", str(status_path)], check=True)
        commit_msg_b = f"Update status.json for PR {file_path.name if file_path else ''} commit {commit_a_sha}"
        subprocess.run(["git", "commit", "-m", commit_msg_b], check=True)
        subprocess.run(["git", "push"], check=True)

        debug_record["result"] = "committed_status_update"
        debug_record["commit_b_sha"] = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True
        ).stdout.strip()
        _save_debug(debug_record)

        return commit_a_sha

    except Exception as e:
        debug_record["result"] = "git_failed"
        debug_record["error"] = str(e)
        _save_debug(debug_record)

        _save_status({
            "target_date": None,
            "state": "failed",
            "downloaded": file_path.name if file_path else None,
            "source": "git_commit_changes",
            "error": str(e)
        })

        print("Unexpected error during git commit:", e)
        return None


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

def download_bhavcopy_yesterday(session_obj: requests.Session, yesterday) -> Path | None:
    """Download yesterday's bhavcopy (PRddmmyy.zip) using NSE Daily Reports API."""
    
    # Holiday check (rarely needed but kept for correctness)
    if nseholiday(yesterday):
        msg = "Yesterday is an NSE holiday – file not expected"
        print(msg)
        try:
            _save_status({
                "target_date": yesterday.strftime("%Y-%m-%d"),
                "state": "holiday",
                "downloaded": None,
                "source": "download_bhavcopy_yesterday",
                "note": msg
            })
            
        except Exception:
            pass
        return None

    file_name = f"PR{yesterday.strftime('%d%m%y')}.zip"

    # Warm cookies
    try:
        session_obj.get(HOME_URL, headers=HEADERS_DICT, timeout=10)
    except Exception:
        pass

    # Call API
    try:
        resp = session_obj.get(DAILY_API_URL, headers=HEADERS_DICT, timeout=20)
    except Exception as e:
        print("Failed to call DAILY_API_URL:", e)
        try:
            _save_debug({"error": str(e), "stage": "api_call_yesterday", "ts": datetime.datetime.now().isoformat()})
            _save_status({
                "target_date": yesterday.strftime("%Y-%m-%d"),
                "state": "failed",
                "downloaded": None,
                "source": "download_bhavcopy_yesterday",
                "error": f"API call failed: {e}"
            })
        except Exception:
            pass
        return None

    if resp.status_code != 200:
        print(f"DAILY_API_URL returned HTTP {resp.status_code}")
        try:
            
            _save_status({
                "target_date": yesterday.strftime("%Y-%m-%d"),
                "state": "failed",
                "downloaded": None,
                "source": "download_bhavcopy_yesterday",
                "error": f"HTTP {resp.status_code}"
            })
        except Exception:
            pass
        return None

    # Parse JSON safely
    data = parse_api_response(resp)
    if not data:
        try:
            _save_status({
                "target_date": yesterday.strftime("%Y-%m-%d"),
                "state": "failed",
                "downloaded": None,
                "source": "download_bhavcopy_yesterday",
                "error": "parse_api_response returned no data"
            })
        except Exception:
            pass
        return None

    reports = collect_all_reports(data)

    # Find yesterday's file
    report_entry = next((r for r in reports if r.get("fileActlName") == file_name), None)
    if not report_entry:
        date_str = yesterday.strftime("%d-%b-%Y")
        report_entry = next((r for r in reports if r.get("tradingDate") == date_str), None)

    if not report_entry:
        print(f"No entry for {file_name} found for yesterday")
        try:
            _save_status({
                "target_date": yesterday.strftime("%Y-%m-%d"),
                "state": "pending",
                "downloaded": None,
                "source": "download_bhavcopy_yesterday",
                "note": "yesterday file not available"
            })
           
        except Exception:
            pass
        return None

    # Download
    file_url = report_entry["filePath"] + report_entry["fileActlName"]
    print("Trying yesterday's file via API:", file_url)

    file_path = _save_file(session_obj, file_name, file_url)
    if not file_path:
        try:
            _save_status({
                "target_date": yesterday.strftime("%Y-%m-%d"),
                "state": "failed",
                "downloaded": None,
                "source": "download_bhavcopy_yesterday",
                "error": "file_save_failed"
            })
        except Exception:
            pass
        return None

    # SUCCESS
    try:
        _save_status({
            "target_date": yesterday.strftime("%Y-%m-%d"),
            "state": "success",
            "downloaded": file_name,
            "downloaded_at": datetime.datetime.now().isoformat(),
            "source": "download_bhavcopy_yesterday"
        })
        _save_debug({
            "event": "download_success_yesterday",
            "file": str(file_path),
            "zip_name": file_name,
            "file_url": file_url,
            "ts": datetime.datetime.now().isoformat()
        })
    except Exception:
        pass

    return file_path


def download_bhavcopy_daybefore(session_obj: requests.Session,day_before) -> Path | None:
    """Download yesterday's bhavcopy (PRddmmyy.zip) using NSE Daily Reports API."""
    
    file_name = f"PR{day_before.strftime('%d%m%y')}.zip"

    # Warm cookies
    try:
        session_obj.get(HOME_URL, headers=HEADERS_DICT, timeout=10)
    except Exception:
        pass

    # Call API
    try:
        resp = session_obj.get(DAILY_API_URL, headers=HEADERS_DICT, timeout=20)
    except Exception as e:
        print("Failed to call DAILY_API_URL:", e)
        _save_debug({"error": str(e)})
        return None

    if resp.status_code != 200:
        print(f"DAILY_API_URL returned HTTP {resp.status_code}")
        _save_debug({
            "status": resp.status_code,
            "headers": dict(resp.headers),
            "body": resp.text[:500]
        })
        return None

    # Parse JSON safely
    data = parse_api_response(resp)
    if not data:
        return None

    reports = collect_all_reports(data)

    # Find yesterday’s file
    report_entry = next((r for r in reports if r.get("fileActlName") == file_name), None)
    if not report_entry:
        date_str = day_before.strftime("%d-%b-%Y")
        report_entry = next((r for r in reports if r.get("tradingDate") == date_str), None)

    if not report_entry:
        print(f"No entry for {file_name} found in DAILY_API_URL response")
        return None

    # Download
    file_url = report_entry["filePath"] + report_entry["fileActlName"]
    print("Trying yesterday file via API:", file_url)
    return _save_file(session_obj, file_name, file_url)

def download_bhavcopy_today(session_obj: requests.Session, today) -> Path | None:
    """Download today's bhavcopy (PRddmmyy.zip) using NSE Daily Reports API."""
    # Holiday check
    if nseholiday(today):
        msg = "Holiday, market closed"
        print(msg)
        try:
            _save_status({
                "target_date": today.strftime("%Y-%m-%d"),
                "state": "holiday",
                "downloaded": None,
                "source": "download_bhavcopy_today",
                "note": "NSE Holiday"
            })
            
        except Exception:
            # don't raise — debugging should not stop the download flow
            pass
        return None

    file_name = f"PR{today.strftime('%d%m%y')}.zip"

    # Warm cookies (best-effort)
    try:
        session_obj.get(HOME_URL, headers=HEADERS_DICT, timeout=10)
    except Exception:
        pass

    # Call API
    try:
        resp = session_obj.get(DAILY_API_URL, headers=HEADERS_DICT, timeout=20)
    except Exception as e:
        print("Failed to call DAILY_API_URL:", e)
        try:
            _save_status({
                "target_date": today.strftime("%Y-%m-%d"),
                "state": "failed",
                "downloaded": None,
                "source": "download_bhavcopy_today",
                "error": f"API call failed: {e}"
            })
        except Exception:
            pass
        return None

    if resp.status_code != 200:
        print(f"DAILY_API_URL returned HTTP {resp.status_code}")
        try:
              _save_status({
                "target_date": today.strftime("%Y-%m-%d"),
                "state": "failed",
                "downloaded": None,
                "source": "download_bhavcopy_today",
                "error": f"HTTP {resp.status_code}"
            })
        except Exception:
            pass
        return None

    # Parse JSON safely
    data = parse_api_response(resp)
    if not data:
        try:
           _save_status({
                "target_date": today.strftime("%Y-%m-%d"),
                "state": "failed",
                "downloaded": None,
                "source": "download_bhavcopy_today",
                "error": "parse_api_response returned no data"
            })
        except Exception:
            pass
        return None

    reports = collect_all_reports(data)

    # Find today's file
    report_entry = next((r for r in reports if r.get("fileActlName") == file_name), None)
    if not report_entry:
        date_str = today.strftime("%d-%b-%Y")
        report_entry = next((r for r in reports if r.get("tradingDate") == date_str), None)

    if not report_entry:
        print(f"No entry for {file_name} found in DAILY_API_URL response")
        try:
            _save_status({
                "target_date": today.strftime("%Y-%m-%d"),
                "state": "pending",
                "downloaded": None,
                "source": "download_bhavcopy_today",
                "note": "today file not available"
            })

        except Exception:
            pass
        return None

    # Download
    file_url = report_entry["filePath"] + report_entry["fileActlName"]
    print("Trying today's file via API:", file_url)

    file_path = _save_file(session_obj, file_name, file_url)
    if not file_path:
        # _save_file handled its own debug logs; write status as failed
        try:
            _save_status({
                "target_date": today.strftime("%Y-%m-%d"),
                "state": "failed",
                "downloaded": None,
                "source": "download_bhavcopy_today",
                "error": "file_save_failed"
            })
        except Exception:
            pass
        return None

    # Success: _save_file returned a Path. Now update status and debug accordingly.
    try:
        # obtain debug filename if you created one for this run; otherwise omit
        _save_status({
            "target_date": today.strftime("%Y-%m-%d"),
            "state": "success",
            "downloaded": file_name,
            "downloaded_at": datetime.datetime.now().isoformat(),
            "source": "download_bhavcopy_today"
        })
        _save_debug({
            "event": "download_success_yesterday",
            "file": str(file_path),
            "zip_name": file_name,
            "file_url": file_url,
            "ts": datetime.datetime.now().isoformat()
        })
        
    except Exception:
        # never allow debug/status writes to break the happy path
        pass

    return file_path


def download_bhavcopy_master(session_obj: requests.Session, mode: str = "auto") -> Path | None:
    """
    Master function with mode support:
      - mode="today":     attempt today's bhavcopy only
      - mode="yesterday": attempt yesterday's bhavcopy only
      - mode="auto":      existing behavior (today -> yesterday -> day-before)
    """
    today = datetime.datetime.now()
    yesterday = adjust_for_weekend(today - timedelta(days=1))
    day_before = adjust_for_weekend(today - timedelta(days=2))

    mode = (mode or "auto").lower()

    # -----------------------------------------
    # MODE: TODAY ONLY
    # -----------------------------------------
    if mode == "today":
        print(f"[MASTER] mode=today -> trying only today {today.strftime('%d-%b-%Y')}")
        return download_bhavcopy_today(session_obj, today)

    # -----------------------------------------
    # MODE: YESTERDAY ONLY
    # -----------------------------------------
    if mode == "yesterday":
        print(f"[MASTER] mode=yesterday -> trying only yesterday {yesterday.strftime('%d-%b-%Y')}")
        return download_bhavcopy_yesterday(session_obj, yesterday)

    # -----------------------------------------
    # MODE: AUTO  (default CLI behavior)
    # Existing logic preserved exactly
    # -----------------------------------------
    print(f"Today: {today.strftime('%d-%b-%Y')}, trying for {yesterday.strftime('%d-%b-%Y')} first")

    # Try today's file
    file_path = download_bhavcopy_today(session_obj, today)
    if file_path:
        return file_path

    # Try yesterday's file
    file_path = download_bhavcopy_yesterday(session_obj, yesterday)
    if file_path:
        return file_path

    # Fallback: day-before-yesterday
    print(f"Yesterday’s file not available, trying {day_before.strftime('%d-%b-%Y')}")
    file_path = download_bhavcopy_daybefore(session_obj, day_before)
    if file_path:
        return file_path

    print("No bhavcopy available for yesterday or day-before-yesterday")
    return None



def nse_is_open() -> bool:
    """Check if yesterday/weekend is a trading day."""
    today = datetime.datetime.now()
    yesterday = today.date() - timedelta(days=1)
    if today.weekday() in (5, 6):
        print("Weekend, market closed")
        return False
    if nseholiday(yesterday):
        print("Holiday, market closed")
        return False
    return True



def run_download_flow(mode: str = "today") -> Path | None:
    """
    Wrapper to call the existing download logic with explicit mode.

    - mode="today":     attempt today's bhavcopy only
    - mode="yesterday": attempt yesterday's bhavcopy only
    - mode="auto":      existing behavior (today -> yesterday -> day-before)

    NOTE:
    - This wrapper REUSES the existing core logic.
    - This wrapper DOES perform git commit (as per decision: keep all commit logic inside downloader).
    """
    mode = (mode or "today").lower()

    # Create a fresh session (existing logic — no change)
    session_obj = establish_browser_session()
    if not session_obj:
        print("[run_download_flow] ERROR: Could not establish NSE session.")
        return None

    print(f"[run_download_flow] mode={mode}")

    # Call master with the selected mode
    file_path = download_bhavcopy_master(session_obj, mode=mode)

    # If download failed
    if not file_path:
        print(f"[run_download_flow] No file downloaded for mode={mode}")
        return None

    # SUCCESS → Commit file to Git
    print(f"[run_download_flow] Download successful: {file_path}")
    git_commit_changes(file_path)

    return file_path
# --- END: wrapper additions ---


def run_from_cli():
    """
    Preserve existing CLI behaviour but routed through this wrapper.
    Keeps the random startup delay, nse_is_open check and commit step as before.
    """
    # existing behaviour preserved: random startup delay, nse_is_open(), establish session, etc.
    startup_delay = random.randint(0, 60)
    print(f"Startup delay: {startup_delay} seconds")
    time.sleep(startup_delay)

    if not nse_is_open():
        print("NSE closed; skipping.")
        return

    # We rely on the existing code's session logic — keep it unchanged.
    session_obj = establish_browser_session()
    if session_obj:
        # Use original 'auto' master behaviour for CLI runs to preserve compatibility
        file_path = download_bhavcopy_master(session_obj)
        if file_path:
            print("Download complete:", file_path)
            # keep existing workflow — caller (CLI path) continues to commit as before
            git_commit_changes(file_path)
        else:
            print("Bhavcopy not available yet. Check debug folder.")
    else:
        print("Failed to establish browser session; exiting.")


# Replace old __main__ behaviour with a call to run_from_cli()
if __name__ == "__main__":
    run_from_cli()
