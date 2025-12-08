#!/usr/bin/env python3
"""
ingest_pr_zip_to_staging.py

Extract PR zip (PR*.zip), parse relevant CSVs (pr, mcap, pd, etc.), create canonical staging rows,
and insert into instruments_master_staging with validation_status='PENDING'.


What it does:
 - opens the zipfile
 - iterates CSV files inside; attempts to parse common columns automatically
 - canonicalizes to staging columns:
     staging_id (DB will assign if autoincrement),
     symbol_raw, series_raw, market_type_raw, mcap_raw, source_file, other_raw (json of original row),
     validation_status='PENDING', validation_errors=NULL, parsed_* = NULL
 - checks if symbol exists in intraday_bhavcopy and includes a boolean column 'present_in_bhav'
 - inserts rows (if not duplicate by symbol+source_file) into instruments_master_staging
 - writes preview CSV to disk
"""

import argparse
import os
import json
import logging
import zipfile
import glob
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from nseintraday_db_utils import connect_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest_pr_zip_to_staging")

# heuristics: candidate columns in various PR files
CANDIDATE_COLS = {
    "symbol": ["SYMBOL", "symbol", "Security", "security", "security_name", "SC_CODE", "symbol_name"],
    "series": ["SERIES", "series"],
    "market_type": ["MKT", "market_type", "mkt_flag", "Mkt"],
    "mcap": ["MCAP", "mcap", "mktcap", "mcap_bucket", "mktcap_bucket"],
    # optionally other columns you want to carry
}



def get_project_root():
    """
    Return project root directory reliably by searching upward until
    we find a folder containing both 'Code' AND 'Output'. This works
    regardless of current working directory or where the script is run from.
    """
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "Code").is_dir() and (parent / "Output").is_dir():
            return str(parent)
    # Fallback: if structure different, attempt to return parent of 'Code' if present
    for parent in p.parents:
        if (parent / "Code").is_dir():
            return str(parent)
    # Worst-case fallback: two levels up (original behaviour)
    return str(p.parents[2])


# ---------------------------------------------------------
# Auto-discovery helpers for PR zip
# ---------------------------------------------------------
def find_latest_pr_zip(project_root):
    """
    Search multiple plausible locations for PR*.zip and return newest match
    or None if none found.
    """
    candidates = []
    search_paths = [
        os.path.join(project_root, "Output", "Intraday"),  # primary
        os.path.join(project_root, "Output"),              # fallback
        # another fallback attempt if running from Code/ subdir
        str(Path(project_root).parent / "Output" / "Intraday")
    ]
    for d in search_paths:
        if os.path.isdir(d):
            pattern = os.path.join(d, "PR*.zip")
            found = glob.glob(pattern)
            if found:
                candidates.extend(found)

    if not candidates:
        logger.debug("No PR zip candidates found in search paths: %s", search_paths)
        return None

    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def resolve_zip_path(project_root, pr_date=None, explicit_zip=None):
    """
    Determine which PR zip to use:
      1) If explicit_zip provided and exists -> use it
      2) If pr_date provided -> construct PR{DDMMYY}.zip (accept both DDMMYY and DDMMYYYY)
      3) Else auto-detect latest PR zip in Output/Intraday (via find_latest_pr_zip)
    """
    # 1) explicit provided
    if explicit_zip:
        if os.path.isfile(explicit_zip):
            return explicit_zip
        # If explicit provided but not absolute, try join with project root
        alt = os.path.join(project_root, explicit_zip)
        if os.path.isfile(alt):
            return alt
        raise FileNotFoundError(f"Explicit zip not found: {explicit_zip}")

    # 2) by pr_date (DDMMYY or DDMMYYYY)
    if pr_date:
        pr_date = pr_date.strip()
        if len(pr_date) == 6:  # DDMMYY
            fname = f"PR{pr_date}.zip"
        elif len(pr_date) == 8:  # DDMMYYYY => convert to DDMMYY by taking last two digits of year
            fname = f"PR{pr_date[0:2]}{pr_date[2:4]}{pr_date[6:8]}.zip"
        else:
            raise ValueError("pr-date must be DDMMYY or DDMMYYYY")
        candidate = os.path.join(project_root, "Output", "Intraday", fname)
        if os.path.isfile(candidate):
            return candidate
        # try fallback locations
        alt1 = os.path.join(project_root, "Output", fname)
        if os.path.isfile(alt1):
            return alt1
        raise FileNotFoundError(f"PR zip for date {pr_date} not found as {candidate} or {alt1}")

    # 3) autodetect newest
    latest = find_latest_pr_zip(project_root)
    if latest:
        return latest

    raise FileNotFoundError("No PR*.zip found under Output/Intraday (searched multiple locations)")

def _resolve_paths_and_engine(args):
    """
    Helper used by main() to compute project_root, zip_path and DB engine.
    Returns (project_root, zip_path, engine, dry, preview)
    """
    # Determine project root robustly
    project_root = get_project_root()
    logger.info("Detected project_root: %s", project_root)

    # Resolve zip path (may raise FileNotFoundError)
    zip_path = resolve_zip_path(project_root, pr_date=args.pr_date, explicit_zip=args.zip)
    logger.info("Using PR zip: %s", zip_path)

    # DB connection (uses your connect_db automatically)
    try:
        engine = connect_db()
    except Exception as e:
        logger.error("DB connection failed: %s", e)
        raise

    # Behavior flags
    # Note: we treat preview-only as forcing dry-run
    dry = bool(args.dry_run or args.preview_only)
    preview = bool(args.preview_only)
    logger.info("Dry run: %s | Preview only: %s", dry, preview)

    return project_root, zip_path, engine, dry, preview

def find_best_col(cols_lower, candidates):
    """Return original column name for first match in candidates list (case-insensitive)."""
    for cand in candidates:
        c = cand.lower()
        if c in cols_lower:
            return cols_lower[c]
    return None

def df_to_staging_rows(df, source_file, symbol_lookup=None):
    """
    Return list of dict rows for insertion into staging.
    For each df row produce:
      symbol_raw, series_raw, market_type_raw, mcap_raw, source_file, other_raw (json)

    symbol_lookup: optional dict mapping SECURITY -> SYMBOL (both normalized) to help populate
                   symbol_raw when the CSV has SECURITY but not SYMBOL (common for PR files).
    """
    rows = []
    if df.empty:
        return rows
    # build map of lower->orig for lookup
    cols_lower = {col.lower(): col for col in df.columns}
    sym_col = find_best_col(cols_lower, CANDIDATE_COLS["symbol"])
    series_col = find_best_col(cols_lower, CANDIDATE_COLS["series"])
    mkt_col = find_best_col(cols_lower, CANDIDATE_COLS["market_type"])
    mcap_col = find_best_col(cols_lower, CANDIDATE_COLS["mcap"])

    # also find SECURITY column (common in pr/pd files) to use with symbol_lookup
    security_col = cols_lower.get("security", None)

    for _, r in df.iterrows():
        symbol_raw = None
        try:
            if sym_col:
                symbol_raw = _norm_val(r.get(sym_col))
            else:
                # If no SYMBOL column but SECURITY present, try to lookup via symbol_lookup
                if security_col:
                    sec_val = _norm_val(r.get(security_col))
                    if sec_val and symbol_lookup:
                        # lookup is case-insensitive (we store normalized keys)
                        symbol_raw = symbol_lookup.get(sec_val.lower())
                    # fallback: use SEC/first col if lookup not available
                    if not symbol_raw:
                        # prefer the SECURITY text if no symbol found
                        symbol_raw = sec_val
                else:
                    # further fallback: try first column value
                    first = df.columns[0]
                    symbol_raw = _norm_val(r.get(first))
        except Exception:
            symbol_raw = None

        series_raw = _norm_val(r.get(series_col)) if series_col else None
        market_type_raw = _norm_val(r.get(mkt_col)) if mkt_col else None
        mcap_raw = _norm_val(r.get(mcap_col)) if mcap_col else None

        # other_raw as JSON: only keep small set of columns to avoid huge blobs
        other = {}
        for c in df.columns:
            if c not in (sym_col, series_col, mkt_col, mcap_col):
                try:
                    val = r.get(c)
                    # convert numpy types to python native
                    if pd.isna(val):
                        continue
                    other[c] = val
                except Exception:
                    continue

        rows.append({
            "symbol_raw": symbol_raw,
            "series_raw": series_raw,
            "market_type_raw": market_type_raw,
            "mcap_raw": mcap_raw,
            "source_file": source_file,
            "other_raw": json.dumps(other, default=str) if other else None,
            "validation_status": "PENDING",
            "validation_errors": None,
            "parsed_symbol": None,
            "parsed_series": None,
            "parsed_market_type": None,
            "parsed_mcap_bucket": None,
            "include_in_bhav": None,
            "fno_flag": False,
            "created_at": datetime.utcnow()
        })
    return rows


def _norm_val(v):
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    # numeric -> string
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return str(v).strip()

def read_csv_from_bytes(b, filename):
    """Read CSV bytes into pandas DataFrame robustly (try a few encodings & separators)."""
    from io import BytesIO, StringIO
    # try common encodings/separators
    attempts = [
        {"encoding": "utf-8", "sep": None},
        {"encoding": "utf-8", "sep": ","},
        {"encoding": "latin-1", "sep": ","},
        {"encoding": "utf-8", "sep": ";"},
    ]
    for a in attempts:
        try:
            # pandas will sniff separator if sep=None
            df = pd.read_csv(BytesIO(b), encoding=a["encoding"], sep=a["sep"], engine="python", dtype=str)
            # normalize columns (strip)
            df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
            return df
        except Exception:
            continue
    # last resort: try read_table with whitespace
    try:
        df = pd.read_csv(BytesIO(b), encoding="latin-1", sep=None, engine="python", dtype=str)
        df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
        return df
    except Exception as e:
        raise

def symbol_present_in_bhav(engine, symbol):
    try:
        sql = text("SELECT 1 FROM intraday_bhavcopy WHERE symbol = :s LIMIT 1")
        df = pd.read_sql(sql, engine, params={"s": symbol})
        return not df.empty
    except Exception:
        # on DB issues be conservative and return False
        return False

def insert_staging_rows(engine, rows, dedupe_on=("symbol_raw", "source_file")):
    """
    Insert rows into instruments_master_staging.
    Avoid duplicate symbol+source_file inserts by checking existing rows first.
    """
    if not rows:
        return 0
    conn = engine.connect()
    inserted = 0
    try:
        # fetch existing symbol+source_file combos
        existing_q = text("SELECT symbol_raw, source_file FROM instruments_master_staging")
        existing_df = pd.read_sql(existing_q, conn)
        existing_set = set((r["symbol_raw"], r["source_file"]) for _, r in existing_df.iterrows())
        # prepare list to insert
        to_insert = []
        for r in rows:
            key = (r["symbol_raw"], r["source_file"])
            if key in existing_set:
                logger.debug("Skipping duplicate staging row: %s / %s", key[0], key[1])
                continue
            to_insert.append(r)
            existing_set.add(key)
        if not to_insert:
            return 0
        # convert to DataFrame and insert via to_sql (use if_exists='append')
        df_ins = pd.DataFrame(to_insert)
        # ensure date/time types are compatible
        df_ins["created_at"] = pd.to_datetime(df_ins["created_at"])
        df_ins.to_sql("instruments_master_staging", conn, index=False, if_exists="append", method="multi", chunksize=500)
        inserted = len(df_ins)
        logger.info("Inserted %d new staging rows", inserted)
    finally:
        conn.close()
    return inserted

def find_csv_files_in_zip(zf):
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    return csv_names

def process_zip_to_staging(zip_path, engine, dry_run=True, export_preview_dir="."):
    """
    Improved process_zip_to_staging:
      - reads all CSVs from zip_path
      - builds PD-based symbol lookup (security -> symbol)
      - builds ETF lookup from any etf*.csv found
      - calls df_to_staging_rows for each CSV to get canonical staging rows
      - attempts to resolve missing symbol_raw using PD lookup and SECURITY inside other_raw
      - annotates other_raw JSON with is_etf / etf_symbol / etf_match_source
      - dedupes (symbol_raw + source_file)
      - does quick present_in_bhav check
      - writes preview CSV; inserts into DB if dry_run is False
    Returns: number of rows inserted (or previewed when dry_run=True)
    """
    if not os.path.exists(zip_path):
        raise FileNotFoundError(zip_path)

    # open zip and list csv members
    zf = zipfile.ZipFile(zip_path, "r")
    csv_members = find_csv_files_in_zip(zf)
    if not csv_members:
        logger.error("No CSV files found inside zip: %s", zip_path)
        return 0

    # --- Build PD symbol lookup (security_lower -> symbol) and ETF sets ---
    pd_symbol_map = {}   # key: security_lower -> value: SYMBOL (canonical)
    etf_symbols = set()
    etf_securities_lower = set()

    # helper to normalize column name matching
    def _find_col(cols, candidates):
        for cand in candidates:
            for c in cols:
                if c.strip().lower() == cand.lower():
                    return c
        return None

    # scan CSVs once to build lookups (non-blocking)
    for member in csv_members:
        lower_name = os.path.basename(member).lower()
        try:
            b = zf.read(member)
            df = read_csv_from_bytes(b, member)
        except Exception:
            # ignore unreadable CSVs for lookup build
            continue

        cols = [c for c in df.columns]

        # build PD map for symbol resolution
        if lower_name.startswith("pd") or "pd" in lower_name:
            # common PD headers: SYMBOL, SECURITY (but be tolerant)
            sym_col = _find_col(cols, ["symbol", "sc_code", "sc_code_2", "SYMBOL"])
            sec_col = _find_col(cols, ["security", "security_name", "SECURITY", "Security"])
            if sym_col and sec_col:
                for _, r in df.iterrows():
                    try:
                        sec = r.get(sec_col)
                        sym = r.get(sym_col)
                        if sec is None or (isinstance(sec, float) and pd.isna(sec)):
                            continue
                        key = str(sec).strip().lower()
                        if key == "":
                            continue
                        if sym is not None and str(sym).strip() != "":
                            pd_symbol_map[key] = str(sym).strip()
                        else:
                            # store None if no SYMBOL, but having security may still be useful
                            if key not in pd_symbol_map:
                                pd_symbol_map[key] = None
                    except Exception:
                        continue

        # build ETF lookups if this appears to be an ETF file
        if "etf" in lower_name:
            # try symbol column or SECURITY column
            sym_col = _find_col(cols, ["symbol", "sc_code", "SYMBOL"])
            sec_col = _find_col(cols, ["security", "SECURITY", "Security", "security_name"])
            for _, r in df.iterrows():
                try:
                    if sym_col:
                        s = r.get(sym_col)
                        if s and str(s).strip() != "":
                            etf_symbols.add(str(s).strip())
                    if sec_col:
                        sec = r.get(sec_col)
                        if sec and str(sec).strip() != "":
                            etf_securities_lower.add(str(sec).strip().lower())
                except Exception:
                    continue

    logger.info("Built symbol_lookup from PD: mappings=%d", len(pd_symbol_map))
    logger.info("Built ETF lookup: etf_symbols=%d etf_securities=%d", len(etf_symbols), len(etf_securities_lower))

    # --- Parse CSVs to staging rows using existing df_to_staging_rows helper ---
    all_rows = []
    for member in csv_members:
        try:
            b = zf.read(member)
            df = read_csv_from_bytes(b, member)
        except Exception as e:
            logger.exception("Failed to read CSV %s inside zip: %s", member, e)
            continue

        rows = df_to_staging_rows(df, source_file=os.path.basename(member))
        logger.info("Prepared %d staging rows from %s", len(rows), member)
        all_rows.extend(rows)

    if not all_rows:
        logger.info("No rows parsed from any CSV inside zip.")
        return 0

    # --- Dedupe by (symbol_raw, source_file) but we also want to keep rows that have no symbol yet (resolve later) ---
    # Keep first occurrence per (symbol_raw, source_file). We'll attempt symbol resolution for missing symbol_raw below.
    seen = set()
    deduped = []
    for r in all_rows:
        key = (r.get("symbol_raw"), r.get("source_file"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    logger.info("After initial dedupe: %d unique staging candidates", len(deduped))

    # --- Resolve missing symbol_raw using PD lookup or SECURITY in other_raw, and annotate ETF info ---
    def _extract_security_from_other(other_json_str):
        if not other_json_str:
            return None
        try:
            j = json.loads(other_json_str) if isinstance(other_json_str, str) else (other_json_str or {})
        except Exception:
            return None
        # look for common keys
        for k in ("SECURITY", "security", "Security", "security_name", "SECURITY_NAME"):
            if k in j and j[k] not in (None, ""):
                return str(j[k]).strip()
        # fallback: find first string-like value with letters and length > 3
        for v in j.values():
            if isinstance(v, str) and len(v.strip()) > 3 and not v.strip().replace(".", "").isdigit():
                return v.strip()
        return None

    resolved_count = 0
    etf_marked = 0
    for r in deduped:
        # ensure other_raw is a JSON string; if not, convert
        other_raw = r.get("other_raw")
        try:
            other = json.loads(other_raw) if isinstance(other_raw, str) and other_raw.strip() != "" else (other_raw if isinstance(other_raw, dict) else {})
        except Exception:
            # keep raw string in fallback field
            other = {"__other_raw_str": str(other_raw)}

        # 1) If symbol_raw missing, try to resolve from PD map using security text in other_raw
        sym = r.get("symbol_raw")
        if not sym or str(sym).strip() == "":
            sec = _extract_security_from_other(other)
            if sec:
                sym_candidate = pd_symbol_map.get(sec.strip().lower())
                if sym_candidate:
                    r["symbol_raw"] = sym_candidate
                    resolved_count += 1
                else:
                    # if pd map had key but None value, leave symbol as None (we still keep the security for manual review)
                    pass

        # 2) Annotate ETF info:
        #   - if symbol_raw matches etf_symbols -> is_etf True
        #   - else if security text in other matches etf_securities_lower -> is_etf True
        is_etf = False
        etf_sym = None
        etf_src = None
        sym_now = r.get("symbol_raw")
        if sym_now and str(sym_now).strip() != "":
            if str(sym_now).strip() in etf_symbols:
                is_etf = True
                etf_sym = str(sym_now).strip()
                etf_src = "symbol"
        if not is_etf:
            sec = _extract_security_from_other(other)
            if sec and sec.strip().lower() in etf_securities_lower:
                is_etf = True
                etf_sym = pd_symbol_map.get(sec.strip().lower()) if sec.strip().lower() in pd_symbol_map else None
                etf_src = "security"

        # store annotation back into other JSON
        other["is_etf"] = bool(is_etf)
        if etf_sym:
            other["etf_symbol"] = etf_sym
        if etf_src:
            other["etf_match_source"] = etf_src

        # persist annotated other_raw back to row as JSON string
        try:
            r["other_raw"] = json.dumps(other, default=str)
        except Exception:
            # fallback to string
            r["other_raw"] = str(other)

        if is_etf:
            etf_marked += 1

    logger.info("Resolved missing symbols via PD lookup: %d", resolved_count)
    logger.info("Annotated ETFs in staging: %d", etf_marked)

    # --- Final dedupe/filter: drop rows that still have no symbol_raw (we cannot insert them usefully) ---
    final_rows = []
    missing_symbol_rows = 0
    for r in deduped:
        s = r.get("symbol_raw")
        if s is None or str(s).strip() == "":
            missing_symbol_rows += 1
            continue
        final_rows.append(r)
    logger.info("Dropped %d staging rows with no resolved symbol; final candidates=%d", missing_symbol_rows, len(final_rows))

    # --- Quick presence-in-bhav check (best-effort) ---
    if engine is not None:
        for r in final_rows:
            try:
                r["present_in_bhav"] = symbol_present_in_bhav(engine, r.get("symbol_raw"))
            except Exception:
                r["present_in_bhav"] = False
    else:
        for r in final_rows:
            r["present_in_bhav"] = False

    # --- Write preview CSV ---
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    preview_path = os.path.join(export_preview_dir, f"staging_preview_{ts}.csv")
    try:
        pd.DataFrame(final_rows).to_csv(preview_path, index=False)
        logger.info("Wrote staging preview CSV: %s", preview_path)
    except Exception as e:
        logger.exception("Failed to write staging preview CSV: %s", e)

    # --- Insert into DB if requested ---
    if dry_run:
        logger.info("Dry-run: skipping DB insert (would insert %d rows)", len(final_rows))
        return len(final_rows)

    try:
        inserted = insert_staging_rows(engine, final_rows)
        logger.info("Inserted %d staging rows into instruments_master_staging", inserted)
        return inserted
    except Exception as e:
        logger.exception("Failed to insert staging rows: %s", e)
        raise




def main():
    p = argparse.ArgumentParser()
    p.add_argument("--zip", required=False, help="Optional: explicit path to PR zip")
    p.add_argument("--pr-date", required=False, help="Optional: DDMMYY or DDMMYYYY")
    p.add_argument("--dry-run", action="store_true", default=True, help="Skip DB writes")
    p.add_argument("--preview-only", action="store_true", default=False, help="Preview only (skip DB writes)")
    p.add_argument("--export-dir", default=".", help="Where to write preview CSV")
    args = p.parse_args()

    # --------------------------------------------
    # Detect project root (nsetradingbot/)
    # --------------------------------------------

    try:
        project_root, zip_path, engine, dry, preview = _resolve_paths_and_engine(args)
    except Exception as e:
        logger.error("Initialization failed: %s", e)
        return

    # --------------------------------------------
    # Call your existing ingestion logic
    # --------------------------------------------
    inserted = process_zip_to_staging(
        zip_path=zip_path,
        engine=engine,
        dry_run=dry,
        export_preview_dir=args.export_dir
    )

    logger.info("Done. rows_inserted_or_previewed=%s", inserted)

if __name__ == "__main__":
    from sqlalchemy import create_engine, text
    main()
