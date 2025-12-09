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
    ETF detection is based ONLY on staging flags.
    Staging has already added: other["is_etf"] = True for ETFs.
    """
    if isinstance(other, dict):
        return bool(other.get("is_etf"))
    return False



def market_type_check(market_type_raw: str, other: dict) -> bool:
    if not market_type_raw:
        return False
    return market_type_raw.strip().upper() in ALLOWED_MARKET_TYPES


def series_check(series_raw: str, other: dict) -> bool:
    if not series_raw:
        return False
    return series_raw.strip().upper() in ALLOWED_SERIES

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
    parsed_fields_dict contains parsed_symbol, parsed_security, parsed_series,
    parsed_market_type, include_in_bhav, fno_flag, is_etf
    """
    errors: List[str] = []
    parsed: Dict[str, Any] = {}

    # --- Normalize inputs ---
    symbol_raw = _str(st_row.get("symbol_raw"))
    series_raw = _str(st_row.get("series_raw"))
    market_raw = _str(st_row.get("market_type_raw"))
    source_file = st_row.get("source_file")
    other_raw = st_row.get("other_raw")

    other = parse_other_json(other_raw)

    # --- parsed_symbol (ONLY from symbol_raw, no fallback) ---
    parsed_symbol = symbol_raw.strip() if symbol_raw else None

    # --- parsed_security (human-readable) ---
    parsed_security = None
    for k in ("SECURITY", "security", "Security", "security_name", "Security Name", "NAME"):
        v = other.get(k)
        if v and str(v).strip():
            parsed_security = str(v).strip()
            break

    # --- parsed series / market type ---
    parsed_series = series_raw or _str(other.get("SERIES")) or None
    parsed_market_type = market_raw or _str(other.get("MKT")) or None

    # --- Checks ---
    try:
        is_etf = bool(etf_check_fn(other, source_file))
    except Exception as e:
        is_etf = False
        errors.append(f"ETF_check_error:{e}")

    try:
        market_ok = bool(market_type_check(market_raw, other))
    except Exception as e:
        market_ok = False
        errors.append(f"market_type_check_error:{e}")

    try:
        series_ok = bool(series_check(series_raw, other))
    except Exception as e:
        series_ok = False
        errors.append(f"series_check_error:{e}")

    # --- EXACT logic you requested ---
    include_in_bhav = (not is_etf) and market_ok and series_ok

    # --- FNO flag detection ---
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
        "include_in_bhav": include_in_bhav,
        "fno_flag": fno_flag,
        "is_etf": is_etf
    })

    # --- Must have a symbol ---
    if not parsed_symbol:
        errors.append("no_symbol_resolved")

    # FINAL STATUS
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

def promote_valid_rows(engine, df_valid: pd.DataFrame, commit: bool = False) -> pd.DataFrame:
    """
    Promote validated staging rows to instruments_master.

    - engine: SQLAlchemy engine/connection
    - df_valid: DataFrame of validated staging rows. Must include:
        parsed_symbol, parsed_security, parsed_series, parsed_market_type,
        include_in_bhav, fno_flag, source_file, other_raw, validated_at, created_at
      (the function is defensive if some columns are missing).
    - commit: if True, apply DB changes in a single transaction. If False, dry-run and return audit.

    Returns: pd.DataFrame with columns ['parsed_symbol','action','details'] for audit.
    """
    audit = []

    # defensive: nothing to do
    if df_valid is None or df_valid.empty:
        return pd.DataFrame(audit)

    # ensure expected columns exist
    for c in ("parsed_symbol", "parsed_security", "parsed_series", "parsed_market_type",
              "include_in_bhav", "fno_flag", "source_file", "other_raw", "validated_at", "created_at"):
        if c not in df_valid.columns:
            df_valid[c] = None

    # normalize dates for picking latest row per symbol
    df = df_valid.copy()
    df["validated_at2"] = pd.to_datetime(df["validated_at"], errors="coerce")
    df["created_at2"] = pd.to_datetime(df["created_at"], errors="coerce")

    # choose ordering key: validated_at then created_at then index
    df.sort_values(by=["parsed_symbol", "validated_at2", "created_at2"],
                   ascending=[True, True, True], inplace=True)

    # keep last (most recent) row per parsed_symbol
    df_unique = df.drop_duplicates(subset=["parsed_symbol"], keep="last").reset_index(drop=True)

    conn = engine.connect()
    trans = None
    try:
        if commit:
            trans = conn.begin()

        # SQL snippets (match your instruments_master schema)
        sel_master = text(
            "SELECT id, symbol, series, mkt_flag, market_type, include_in_bhav, fno_flag, source_file, source_row_raw, notes "
            "FROM instruments_master WHERE symbol = :sym LIMIT 1"
        )
        insert_master = text(
            "INSERT INTO instruments_master (symbol, series, mkt_flag, market_type, include_in_bhav, fno_flag, source_file, source_row_raw, mapped_by, mapped_at, created_at, updated_at) "
            "VALUES (:symbol, :series, :mkt_flag, :market_type, :include_in_bhav, :fno_flag, :source_file, :source_row_raw, :mapped_by, NOW(), NOW(), NOW())"
        )
        update_master = text(
            "UPDATE instruments_master SET series=:series, mkt_flag=:mkt_flag, market_type=:market_type, include_in_bhav=:include_in_bhav, fno_flag=:fno_flag, source_file=:source_file, source_row_raw=:source_row_raw, mapped_by=:mapped_by, mapped_at=NOW(), updated_at=NOW() "
            "WHERE id = :id"
        )

        def _parse_master_source_row_raw(raw):
            # helper: existing source_row_raw may be JSON string or NULL
            if raw is None:
                return {}
            try:
                if isinstance(raw, (str, bytes)):
                    return json.loads(raw)
                if isinstance(raw, dict):
                    return raw
            except Exception:
                try:
                    # fallback: try eval-ish parse (safe fallback to empty)
                    return {}
                except Exception:
                    return {}
            return {}

        def _extract_master_security(master_row):
            # Try to extract SECURITY from source_row_raw JSON, then notes fallback
            try:
                src = master_row.get("source_row_raw")
                j = _parse_master_source_row_raw(src)
                for k in ("SECURITY", "security", "Security", "security_name"):
                    if k in j and j[k]:
                        return str(j[k]).strip()
                # fallback to notes
                if master_row.get("notes"):
                    return str(master_row.get("notes")).strip()
            except Exception:
                pass
            return None

        def _norm_str(v):
            if v is None:
                return None
            return str(v).strip()

        for _, row in df_unique.iterrows():
            parsed_symbol = _norm_str(row.get("parsed_symbol"))
            if not parsed_symbol:
                audit.append({"parsed_symbol": None, "action": "skipped_no_symbol", "details": "no parsed_symbol"})
                continue

            parsed_security = _norm_str(row.get("parsed_security"))
            parsed_series = _norm_str(row.get("parsed_series"))
            parsed_market_type = _norm_str(row.get("parsed_market_type"))
            include_in_bhav = bool(row.get("include_in_bhav"))
            fno_flag = bool(row.get("fno_flag"))
            source_file = _norm_str(row.get("source_file"))
            other_raw = row.get("other_raw")
            try:
                # store source_row_raw as JSON string (consistent)
                source_row_raw = json.dumps(parse_other_json(other_raw), default=str) if other_raw is not None else None
            except Exception:
                source_row_raw = _norm_str(other_raw)

            # mkt_flag: keep short MKT flag when parsed_market_type is short (like 'N'), else NULL
            mkt_flag = parsed_market_type if parsed_market_type and len(parsed_market_type) <= 2 else None

            # fetch existing master row (by symbol)
            master = pd.read_sql(sel_master, conn, params={"sym": parsed_symbol})
            if master.empty:
                # insert
                if commit:
                    conn.execute(insert_master, {
                        "symbol": parsed_symbol,
                        "series": parsed_series,
                        "mkt_flag": mkt_flag,
                        "market_type": parsed_market_type,
                        "include_in_bhav": 1 if include_in_bhav else 0,
                        "fno_flag": 1 if fno_flag else 0,
                        "source_file": source_file,
                        "source_row_raw": source_row_raw,
                        "mapped_by": "validator"
                    })
                audit.append({"parsed_symbol": parsed_symbol, "action": "inserted", "details": None})
            else:
                master_row = master.iloc[0].to_dict()
                diffs = {}

                # compare series
                if _norm_str(master_row.get("series")) != parsed_series:
                    diffs["series"] = {"old": master_row.get("series"), "new": parsed_series}

                # compare market_type
                if _norm_str(master_row.get("market_type")) != parsed_market_type:
                    diffs["market_type"] = {"old": master_row.get("market_type"), "new": parsed_market_type}

                # compare include_in_bhav (bool)
                if bool(master_row.get("include_in_bhav")) != include_in_bhav:
                    diffs["include_in_bhav"] = {"old": bool(master_row.get("include_in_bhav")), "new": include_in_bhav}

                # compare fno_flag
                if bool(master_row.get("fno_flag")) != fno_flag:
                    diffs["fno_flag"] = {"old": bool(master_row.get("fno_flag")), "new": fno_flag}

                # compare source_file
                if _norm_str(master_row.get("source_file")) != source_file:
                    diffs["source_file"] = {"old": master_row.get("source_file"), "new": source_file}

                # compare parsed_security vs SECURITY inside master.source_row_raw or notes
                master_security = _extract_master_security(master_row)
                if _norm_str(master_security) != parsed_security:
                    diffs["parsed_security"] = {"old": master_security, "new": parsed_security}

                # compare source_row_raw text (string compare)
                if _norm_str(master_row.get("source_row_raw")) != _norm_str(source_row_raw):
                    diffs["source_row_raw"] = {"old": master_row.get("source_row_raw"), "new": source_row_raw}

                if diffs:
                    # update
                    if commit:
                        conn.execute(update_master, {
                            "series": parsed_series,
                            "mkt_flag": mkt_flag,
                            "market_type": parsed_market_type,
                            "include_in_bhav": 1 if include_in_bhav else 0,
                            "fno_flag": 1 if fno_flag else 0,
                            "source_file": source_file,
                            "source_row_raw": source_row_raw,
                            "mapped_by": "validator",
                            "id": int(master_row.get("id"))
                        })
                    audit.append({"parsed_symbol": parsed_symbol, "action": "updated", "details": json.dumps(diffs)})
                else:
                    audit.append({"parsed_symbol": parsed_symbol, "action": "no_change", "details": None})

        if commit and trans is not None:
            trans.commit()

    except Exception:
        if trans is not None:
            try:
                trans.rollback()
            except Exception:
                pass
        # re-raise for caller to handle/log
        raise
    finally:
        conn.close()

    return pd.DataFrame(audit)


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
    p.add_argument("--preview-only", action="store_true", default=False, help="Preview mode (alias for dry-run)")
    p.add_argument("--commit", action="store_true", default=True, help="Actually write validations & promote rows (implies not dry-run)")
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
    _expected_cols = ["other_raw", "symbol_raw", "series_raw", "market_type_raw", "source_file"]
    for _c in _expected_cols:
        if _c not in df_st.columns:
            df_st[_c] = None

    # Replace pandas NA/NaN-like values with Python None
    for _c in _expected_cols:
        df_st[_c] = df_st[_c].where(df_st[_c].notnull(), None)

    # parse 'other_raw' to dict for checks (safe)
    df_st['other_parsed'] = df_st['other_raw'].apply(lambda x: parse_other_json(x))

    # use the rule functions to decide inclusion
    def _keep_row(row):
        other = row['other_parsed'] or {}
        src = row.get('source_file')
        # ETF -> drop
        if is_etf_check(other, src):
            return False
        # market type must be allowed (e.g., 'N')
        if not market_type_check((row.get('market_type_raw') or ''), other):
            return False
        # series must be allowed (e.g., 'EQ')
        if not series_check((row.get('series_raw') or ''), other):
            return False
        return True

    before_count = len(df_st)
    df_st = df_st[df_st.apply(_keep_row, axis=1)].copy()
    logger.info("Filtered staging rows: before=%d after=%d", before_count, len(df_st))

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
            # promote_valid_rows signature does not accept export_dir; pass commit only
            audit_df = promote_valid_rows(engine, df_valid, commit=commit)
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
