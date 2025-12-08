#!/usr/bin/env python3
"""
nseintraday_pr_validator.py

Validator & promoter for instruments_master_staging -> instruments_master

Behavior (safe defaults):
 - Reads staging rows with validation_status = 'PENDING' (or supplied status)
 - Runs validation rules (ETF detection, market_type, series)
 - Writes CSVs:
     - {date}_error.csv  -> invalid rows + reasons
     - {date}_Audit.csv  -> rows that would be inserted/updated into instruments_master
 - Promotion into instruments_master happens inside a single DB transaction when --commit is passed.
 - Default: dry-run, no DB writes.
"""

import argparse
import json
import logging
import os
import glob
from datetime import datetime
from typing import Tuple, Dict, List, Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Import your DB helper
from nseintraday_db_utils import connect_db

# ---------------------------
# Config — editable
# ---------------------------
ALLOWED_MARKET_TYPES = {"N"}        # PR.MKT 'N' => mainboard (use 'N' as canonical)
ALLOWED_SERIES = {"EQ"}             # Series allowed (EQ)
ETF_KEYWORDS = {"etf", "exchange traded fund", "goldbees", "liquidbees", "silverbees"}
# You can extend ETF_KEYWORDS to match any ETF naming patterns you encounter.

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger = logging.getLogger("pr_validator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def parse_other_json(other_raw: Any) -> dict:
    """Safely parse other_raw (which may be JSON string or already dict)."""
    if other_raw is None:
        return {}
    if isinstance(other_raw, dict):
        return other_raw
    # Try to load JSON; fall back to attempt minimal cleaning
    try:
        return json.loads(other_raw)
    except Exception:
        try:
            # sometimes pandas stores strings with single quotes; be conservative
            cleaned = other_raw.strip()
            if cleaned.startswith("{") and cleaned.endswith("}"):
                return json.loads(cleaned.replace("'", '"'))
        except Exception:
            pass
    return {}

def _str(v):
    return None if v is None else str(v).strip()

# ---------------------------------------------------------------------------
# Rule functions (copy/paste-friendly to update later)
# ---------------------------------------------------------------------------
def is_etf_check(other: dict, source_file: str) -> bool:
    """
    Return True if instrument is an ETF.
    Heuristics (order matters):
      1) If source_file looks like an ETF file (e.g., startswith "etf") -> True.
      2) If explicit flags exist in `other` (keys like 'is_etf','ETF','security_type').
      3) If SECURITY / name contains ETF keywords.
    """
    src = (source_file or "").lower()
    if src.startswith("etf") or "/etf" in src or "\\etf" in src:
        return True

    if isinstance(other, dict):
        # keys that may indicate ETF boolean or a label
        for key in ("is_etf", "ETF", "etf", "fund_type", "security_type"):
            if key in other:
                try:
                    v = str(other.get(key)).strip().lower()
                    if v in ("y", "yes", "true", "1", "etf", "exchange traded fund"):
                        return True
                except Exception:
                    continue

        # check name fields for ETF keywords
        for cand_key in ("SECURITY", "security", "Security", "NAME", "name", "security_name"):
            val = other.get(cand_key)
            if isinstance(val, str) and val.strip():
                s = val.strip().lower()
                for kw in ETF_KEYWORDS:
                    if kw in s:
                        return True

        # check any other string field for ETF keyword as fallback (conservative)
        for v in other.values():
            if isinstance(v, str):
                s = v.strip().lower()
                for kw in ETF_KEYWORDS:
                    if kw in s:
                        return True
    return False

def market_type_check(parsed_market_type: str, other: dict) -> bool:
    """
    Return True if parsed_market_type is allowed.
    Accepts:
      - direct canonical tokens like 'N'
      - textual tokens like 'MAINBOARD', 'EQ', 'EQUITY' (best-effort)
    """
    if not parsed_market_type:
        return False
    t = str(parsed_market_type).strip().upper()

    # Accept primary canonical code directly (N)
    if t in ALLOWED_MARKET_TYPES:
        return True

    # Some files may contain descriptive values: try mapping
    if t in ("MAINBOARD", "MAIN", "EQUITY", "EQ"):
        # Map textual equity/mainboard to allowed result if ALLOWED_MARKET_TYPES contains the canonical
        if "N" in ALLOWED_MARKET_TYPES or "MAINBOARD" in ALLOWED_MARKET_TYPES:
            return True

    # not allowed
    return False

def series_check(parsed_series: str, other: dict) -> bool:
    """
    Return True if parsed series is allowed (e.g., 'EQ').
    Accepts exact or textual tokens (EQ, EQUITY).
    """
    if not parsed_series:
        return False
    s = str(parsed_series).strip().upper()
    if s in ALLOWED_SERIES:
        return True
    # textual variations
    if s in ("EQUITY", "EQ"):
        return True
    return False

# ---------------------------------------------------------------------------
# Validation core
# ---------------------------------------------------------------------------
def validate_row(st_row: dict,
                 etf_check_fn=is_etf_check,
                 market_check_fn=market_type_check,
                 series_check_fn=series_check) -> Tuple[bool, List[str], dict]:
    """
    Validate a single staging row.
    Returns: (is_valid, errors_list, parsed_fields_dict)
    parsed_fields_dict contains parsed_symbol, parsed_security, parsed_series, parsed_market_type, include_in_bhav, fno_flag, is_etf
    """
    errors: List[str] = []
    parsed: Dict[str, Any] = {}

    # inputs (normalize)
    symbol_raw = _str(st_row.get("symbol_raw"))
    series_raw = _str(st_row.get("series_raw"))
    market_raw = _str(st_row.get("market_type_raw"))
    source_file = st_row.get("source_file")
    other_raw = st_row.get("other_raw")

    other = parse_other_json(other_raw)

    # resolve parsed_symbol: prefer explicit symbol_raw, else try PD 'SYMBOL' or other keys
    parsed_symbol = None
    if symbol_raw:
        parsed_symbol = symbol_raw
    else:
        # try common columns inside other
        for k in ("SYMBOL", "symbol", "SC_CODE", "sc_code", "Scrip", "SCRIP"):
            v = other.get(k)
            if v and str(v).strip():
                parsed_symbol = str(v).strip()
                break

    # parsed_security from other (human-readable name)
    parsed_security = None
    for k in ("SECURITY", "security", "Security", "security_name", "Security Name", "NAME"):
        v = other.get(k)
        if v and str(v).strip():
            parsed_security = str(v).strip()
            break

    # parsed series / market
    parsed_series = series_raw or _str(other.get("SERIES")) or None
    parsed_market_type = market_raw or _str(other.get("MKT")) or None

    # ETF check
    try:
        is_etf = bool(etf_check_fn(other, source_file))
    except Exception as e:
        is_etf = False
        errors.append(f"ETF_check_error:{e}")

    # market & series checks
    try:
        market_ok = bool(market_check_fn(parsed_market_type, other))
    except Exception as e:
        market_ok = False
        errors.append(f"market_check_error:{e}")

    try:
        series_ok = bool(series_check_fn(parsed_series, other))
    except Exception as e:
        series_ok = False
        errors.append(f"series_check_error:{e}")

    # decide include_in_bhav: ETF -> False, else require both market_ok & series_ok
    include_in_bhav = False
    if is_etf:
        include_in_bhav = False
    elif market_ok and series_ok:
        include_in_bhav = True
    else:
        include_in_bhav = False

    # fno flag: check common keys (user may enrich this later)
    fno_flag = False
    try:
        fno_flag = bool(other.get("FNO") or other.get("fno") or other.get("is_fno"))
    except Exception:
        fno_flag = False

    parsed.update({
        "parsed_symbol": parsed_symbol,
        "parsed_security": parsed_security,
        "parsed_series": parsed_series,
        "parsed_market_type": parsed_market_type,
        "include_in_bhav": bool(include_in_bhav),
        "fno_flag": bool(fno_flag),
        "is_etf": bool(is_etf)
    })

    # Basic validations to mark INVALID: must have resolved symbol
    if not parsed_symbol:
        errors.append("no_symbol_resolved")

    # Compose is_valid
    is_valid = (len(errors) == 0)

    return is_valid, errors, parsed

# ---------------------------------------------------------------------------
# DB helpers & promotion logic (mostly unchanged)
# ---------------------------------------------------------------------------
def load_pending_staging(engine, status="PENDING", limit: int | None = None) -> pd.DataFrame:
    q = "SELECT * FROM instruments_master_staging WHERE validation_status = :status ORDER BY created_at"
    if limit:
        q = q + " LIMIT :lim"
        df = pd.read_sql(text(q), engine, params={"status": status, "lim": int(limit)})
    else:
        df = pd.read_sql(text(q), engine, params={"status": status})
    return df

def promote_valid_rows(engine, df_valid: pd.DataFrame, commit: bool = False, export_dir: str = ".") -> pd.DataFrame:
    """
    Promote valid staging rows to instruments_master.
    - Runs in a single transaction when commit=True.
    - Returns DataFrame of actions (insert/update/skipped) for audit CSV.
    """
    audit_rows = []
    conn = engine.connect()
    trans = None
    try:
        if commit:
            trans = conn.begin()
        # prepare statements
        sel_sql = text("SELECT * FROM instruments_master WHERE parsed_symbol = :sym LIMIT 1")
        insert_sql = text(
            "INSERT INTO instruments_master (parsed_symbol, parsed_security, parsed_series, parsed_market_type, include_in_bhav, fno_flag, source_file, other_raw, updated_at, created_at) "
            "VALUES (:parsed_symbol, :parsed_security, :parsed_series, :parsed_market_type, :include_in_bhav, :fno_flag, :source_file, :other_raw, NOW(), NOW())"
        )
        update_sql = text(
            "UPDATE instruments_master SET parsed_security=:parsed_security, parsed_series=:parsed_series, parsed_market_type=:parsed_market_type, "
            "include_in_bhav=:include_in_bhav, fno_flag=:fno_flag, source_file=:source_file, other_raw=:other_raw, updated_at=NOW() "
            "WHERE parsed_symbol=:parsed_symbol"
        )

        for _, r in df_valid.iterrows():
            parsed_symbol = r.get("parsed_symbol")
            if not parsed_symbol:
                audit_rows.append({"parsed_symbol": None, "action": "skipped_no_symbol", "details": "no parsed_symbol"})
                continue

            # get existing
            exist = pd.read_sql(sel_sql, conn, params={"sym": parsed_symbol})
            row_payload = {
                "parsed_symbol": parsed_symbol,
                "parsed_security": r.get("parsed_security"),
                "parsed_series": r.get("parsed_series"),
                "parsed_market_type": r.get("parsed_market_type"),
                "include_in_bhav": bool(r.get("include_in_bhav")),
                "fno_flag": bool(r.get("fno_flag")),
                "source_file": r.get("source_file"),
                "other_raw": json.dumps(parse_other_json(r.get("other_raw")), default=str)
            }

            if exist.empty:
                # insert
                if commit:
                    conn.execute(insert_sql, row_payload)
                audit_rows.append({"parsed_symbol": parsed_symbol, "action": "inserted", "details": None})
            else:
                # compare fields to decide update
                exist_row = exist.iloc[0].to_dict()
                diffs = {}
                for k in ("parsed_security", "parsed_series", "parsed_market_type", "include_in_bhav", "fno_flag", "source_file", "other_raw"):
                    newv = row_payload.get(k)
                    oldv = exist_row.get(k)
                    if isinstance(newv, str) and isinstance(oldv, str):
                        if newv.strip() != oldv.strip():
                            diffs[k] = {"old": oldv, "new": newv}
                    else:
                        if newv != oldv:
                            diffs[k] = {"old": oldv, "new": newv}
                if diffs:
                    if commit:
                        conn.execute(update_sql, row_payload)
                    audit_rows.append({"parsed_symbol": parsed_symbol, "action": "updated", "details": json.dumps(diffs)})
                else:
                    audit_rows.append({"parsed_symbol": parsed_symbol, "action": "no_change", "details": None})

        if commit:
            trans.commit()
    except Exception:
        if trans is not None:
            trans.rollback()
        raise
    finally:
        conn.close()

    return pd.DataFrame(audit_rows)

def write_csv(df: pd.DataFrame, path: str):
    try:
        df.to_csv(path, index=False)
    except Exception:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("failed_to_write_csv\n")

# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None, help="Limit number of staging rows to process")
    p.add_argument("--status", default="PENDING", help="staging validation_status to consume (default: PENDING)")
    p.add_argument("--dry-run", action="store_true", default=True, help="Dry run: skip DB writes (default True) ,False will commit to DB")
    p.add_argument("--preview-only", action="store_true", default=False, help="Preview mode (alias for dry-run)")
    p.add_argument("--commit", action="store_true", default=False, help="Actually write validations & promote rows (implies not dry-run)")
    p.add_argument("--export-dir", default=".", help="Directory to write audit/error CSVs")
    args = p.parse_args()

    # semantics: if --commit passed, we will persist; else dry-run.
    commit = bool(args.commit)
    dry_run = not commit

    logger.info("Validator start. dry_run=%s, commit=%s, status=%s, limit=%s", dry_run, commit, args.status, args.limit)

    engine = connect_db()

    # 1) load staging rows pending
    df_st = load_pending_staging(engine, status=args.status, limit=args.limit)
    if df_st.empty:
        logger.info("No staging rows to validate (status=%s)", args.status)
        return

    logger.info("Loaded %d staging rows for validation", len(df_st))

    # normalize
    df_st = df_st.fillna(value={"other_raw": None, "symbol_raw": None, "series_raw": None, "market_type_raw": None, "source_file": None})

    # outputs
    invalid_rows = []
    valid_rows = []

    # iterate rows and validate
    for _, r in df_st.iterrows():
        rdict = r.to_dict()
        is_valid, errs, parsed = validate_row(rdict)
        # append parsed fields to row dict for later promotion / db write
        for k, v in parsed.items():
            rdict[k] = v
        rdict["validation_errors"] = None if is_valid else ";".join(errs)
        rdict["validation_status"] = "VALID" if is_valid else "INVALID"
        if is_valid:
            valid_rows.append(rdict)
        else:
            invalid_rows.append(rdict)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    err_csv = os.path.join(args.export_dir, f"{ts}_error.csv")
    audit_csv = os.path.join(args.export_dir, f"{ts}_Audit.csv")

    # write invalid CSV
    if invalid_rows:
        df_err = pd.DataFrame(invalid_rows)
        write_csv(df_err, err_csv)
        logger.info("Wrote %d invalid rows to %s", len(df_err), err_csv)
    else:
        logger.info("No invalid rows")

    # produce audit preview and optionally promote
    if valid_rows:
        df_valid = pd.DataFrame(valid_rows)
        try:
            audit_df = promote_valid_rows(engine, df_valid, commit=commit, export_dir=args.export_dir)
        except Exception as e:
            logger.exception("Promotion failed: %s", e)
            raise

        write_csv(audit_df, audit_csv)
        logger.info("Wrote audit CSV: %s (rows=%d)", audit_csv, len(audit_df))
    else:
        logger.info("No valid rows to promote")

    # If commit was requested: update staging validation_status/parsed fields in DB
    if commit:
        try:
            conn = engine.connect()
            trans = conn.begin()
            for row in invalid_rows + valid_rows:
                pk = row.get("staging_id") or row.get("id")
                if not pk:
                    # fallback update by symbol_raw + source_file
                    upd_sql = text(
                        "UPDATE instruments_master_staging SET validation_status=:status, validation_errors=:errs, parsed_symbol=:ps, parsed_security=:psec, parsed_series=:pseries, parsed_market_type=:pmkt, include_in_bhav=:inc, fno_flag=:fno WHERE symbol_raw=:sym AND source_file=:src"
                    )
                    conn.execute(upd_sql, {
                        "status": row.get("validation_status"),
                        "errs": row.get("validation_errors"),
                        "ps": row.get("parsed_symbol"),
                        "psec": row.get("parsed_security"),
                        "pseries": row.get("parsed_series"),
                        "pmkt": row.get("parsed_market_type"),
                        "inc": bool(row.get("include_in_bhav")),
                        "fno": bool(row.get("fno_flag")),
                        "sym": row.get("symbol_raw"),
                        "src": row.get("source_file")
                    })
                else:
                    upd_sql2 = text(
                        "UPDATE instruments_master_staging SET validation_status=:status, validation_errors=:errs, parsed_symbol=:ps, parsed_security=:psec, parsed_series=:pseries, parsed_market_type=:pmkt, include_in_bhav=:inc, fno_flag=:fno WHERE id=:id"
                    )
                    conn.execute(upd_sql2, {
                        "status": row.get("validation_status"),
                        "errs": row.get("validation_errors"),
                        "ps": row.get("parsed_symbol"),
                        "psec": row.get("parsed_security"),
                        "pseries": row.get("parsed_series"),
                        "pmkt": row.get("parsed_market_type"),
                        "inc": bool(row.get("include_in_bhav")),
                        "fno": bool(row.get("fno_flag")),
                        "id": pk
                    })
            trans.commit()
            conn.close()
            logger.info("Staging rows updated with validation_status/parsed fields (commit applied).")
        except Exception:
            logger.exception("Failed updating staging rows after commit")
            raise
    else:
        logger.info("Dry-run: NOT updating staging DB rows (use --commit to persist changes).")

    logger.info("Validator finished.")

if __name__ == "__main__":
    main()
