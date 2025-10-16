"""
cand_pipeline_modular.py

Modular candidate pipeline with single-responsibility functions:
- configload()
- candidatelistgeneration()
- writecandidatelisttocsv()
- Updatecandidatelistcsv()
- upsert_candidatelist()

"""

import os
import urllib.parse
from sqlalchemy import create_engine, text
import re
import io
import sys
import json
import yaml
import zipfile
import logging
import argparse
import math
from datetime import datetime
from typing import Tuple, List
import numpy as np
import pandas as pd
from dateutil import parser as dateutil_parser


# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("cand_pipeline_modular")

# ---------------------------
# Defaults (change as needed)
# ---------------------------
DEFAULT_CONFIG = {
    "db": {
        "host": "localhost",
        "user": "root",
        "password": "your_password",
        "db": "intradaytrading",
        "port": 3306
    },
    "thresholds": {
        "candidate_pct_threshold": 2.0,
        "candidate_top_n": 50,
        "tt_top_n": 20
    },
    "paths": {
        "output_root": "./Output/Intraday"
    },
    "mappings": {
        "GL": {"security": "SECURITY", "pct": ["PERCENT_CG", "PERCENT_CHG"], "close": ["CLOSE_PRIC", "CLOSE_PRICE"], "prev_close": "PREV_CL_PR"},
        "HL": {"security": "SECURITY", "status": "NEW_STATUS", "new": "NEW", "previous": "PREVIOUS"},
        "PR": {"security": "SECURITY", "mkt": "MKT", "prev_close": "PREV_CL_PR", "open": "OPEN_PRICE", "high": "HIGH_PRICE", "low": "LOW_PRICE", "close": "CLOSE_PRICE", "net_trdval": "NET_TRDVAL", "net_trdqty": "NET_TRDQTY", "trades": "TRADES", "ind_sec": "IND_SEC", "corp_ind": "CORP_IND", "hi_52_wk": "HI_52_WK", "lo_52_wk": "LO_52_WK"},
        "TT": {"security": "SECURITY", "net_trdval": ["NET_TRDVAL", "NET_TRD_VAL"], "net_trdqty": ["NET_TRDQTY", "NET_TRD_QTY"]},
        "ETF": {"security": "SYMBOL"}
    },
    "behavior": {
        "exclude_etf": True,
        "exclude_sme": True,
        "preview_only": False, #If True, the script skips the final DB-upsert stage (upsert_candidatelist() for intraday_bhavcopy). If False, it runs the full pipeline including that final step.
        "dry_run": True  # if True, DB writes are rolled back; if False, DB writes are committed
        #For validation purposes, set dry_run to True and preview_only to False to run the full pipeline without committing changes.
        #For production use, set dry_run to False and preview_only to False to run the full pipeline and commit changes.
    },
    "output": {
        "candidates_csv": "candidates_preview.csv"
    }
}

# ---------------------------
# Utilities
# ---------------------------

def load_config(path: str = None) -> dict:
    if not path:
        return DEFAULT_CONFIG.copy()
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        if path.lower().endswith(('.yaml', '.yml')):
            cfg = yaml.safe_load(f)
        else:
            cfg = json.load(f)
    merged = DEFAULT_CONFIG.copy()
    if cfg:
        for k, v in cfg.items():
            if isinstance(v, dict) and k in merged:
                merged[k].update(v)
            else:
                merged[k] = v
    return merged

def connect_db(db_cfg: dict = None):
    """
    Create and return a SQLAlchemy engine. db_cfg overrides environment variables if provided.
    """
    # Credentials: prefer MYSQL_PASSWORD env over config password
    env_password = os.getenv('MYSQL_PASSWORD')
    password = env_password if env_password is not None else (db_cfg.get("password") if db_cfg else "")
    encoded_pw = urllib.parse.quote_plus(password)

    user = db_cfg.get("user", "root") if db_cfg else "root"
    host = db_cfg.get("host", "localhost") if db_cfg else "localhost"
    port = db_cfg.get("port", 3306) if db_cfg else 3306
    dbname = db_cfg.get("db", "intradaytrading") if db_cfg else "intradaytrading"

    DATABASE_URL = f"mysql+pymysql://{user}:{encoded_pw}@{host}:{port}/{dbname}"

    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )
    logger.info("SQLAlchemy engine created for DB: %s", dbname)
    return engine


def read_csv_from_zip(z: zipfile.ZipFile, name: str) -> pd.DataFrame:
    raw = z.read(name).decode('utf-8', errors='replace')
    df = pd.read_csv(io.StringIO(raw), engine='python', on_bad_lines='skip')
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df

def find_files_in_zip(zip_path: str) -> dict:
    with zipfile.ZipFile(zip_path, 'r') as z:
        names = z.namelist()
    found = {"GL": [], "HL": [], "PR": [], "TT": [], "ETF": [], "OTHER": []}
    for n in names:
        base = os.path.basename(n).upper()
        if base.startswith("GL") and base.endswith(".CSV"):
            found["GL"].append(n)
        elif base.startswith("HL") and base.endswith(".CSV"):
            found["HL"].append(n)
        elif base.startswith("PR") and base.endswith(".CSV"):
            found["PR"].append(n)
        elif base.startswith("TT") and base.endswith(".CSV"):
            found["TT"].append(n)
        elif base.startswith("ETF") and base.endswith(".CSV"):
            found["ETF"].append(n)
        else:
            found["OTHER"].append(n)
    return found

def safe_first_existing_col(df: pd.DataFrame, names):
    for n in names:
        if n and n.upper() in df.columns:
            return n.upper()
    return None

def to_float_safe(x):
    try:
        if pd.isna(x):
            return None
        s = str(x).strip().replace(',', '')
        if s.endswith('%'):
            s = s[:-1]
        if s == '':
            return None
        return float(s)
    except Exception:
        return None

def normalize_symbol(sym):
    if sym is None:
        return None
    return str(sym).strip().upper()

# JSON sanitization helpers
def _make_json_serializable(value):
    """Normalize pandas / numpy / python values into JSON-safe values."""
    if value is None:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    s = str(value).strip()
    if s == "" or s.lower() in ("nan", "none", "null", "n/a"):
        return None
    try:
        if "." in s:
            f = float(s.replace(",", ""))
            if not math.isnan(f) and not math.isinf(f):
                return f
        else:
            i = int(s.replace(",", ""))
            return i
    except Exception:
        pass
    return s

def safe_json_dumps(obj: dict) -> str:
    """Convert a dictionary to a JSON string with safe conversion of values."""
    serializable = {}
    for k, v in obj.items():
        serializable[k] = _make_json_serializable(v)
    try:
        text = json.dumps(serializable, ensure_ascii=False)
        json.loads(text)
        return text
    except Exception as e:
        logger.warning("safe_json_dumps fallback: %s", e)
        return json.dumps({k: (None if serializable[k] is None else str(serializable[k])) for k in serializable}, ensure_ascii=False)

def build_extras_json_from_series(row_series: pd.Series) -> str:
    """Build a strict JSON 'extras' string from a pandas Series (row)."""
    skip = {"trade_date", "symbol", "mkt", "mkt_flag", "prev_close", "open", "high", "low", "close",
            "net_trdval", "net_trdqty", "trades", "ind_sec", "corp_ind", "hi_52_wk", "lo_52_wk"}
    extras = {}
    for column in row_series.index:
        if column in skip:
            continue
        extras[column] = _make_json_serializable(row_series.get(column))
    extras_json = json.dumps(extras, ensure_ascii=False)
    json.loads(extras_json)
    return extras_json
def none_if_nan(val):
    """Return None for NaN/inf-like values; otherwise return original value."""
    try:
        if val is None:
            return None
        # pandas / numpy NaN check
        if pd.isna(val):
            return None
        # float inf check
        if isinstance(val, (float,)) and (math.isinf(val)):
            return None
        return val
    except Exception:
        return None
# ---------------------------
# Robust date extraction helpers
# ---------------------------
COMMON_DATE_COL_NAMES = [
    "TRADE_DATE", "TRADE DATE", "DATE", "AS_OF_DATE", "AS OF", "AS_OF", "REPORT_DATE", "FILE_DATE"
]

def _parse_date_value(v):
    """Try to parse a single value into an ISO date (YYYY-MM-DD). Returns str or None."""
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None

        # Numeric 8-digit YYYYMMDD
        if s.isdigit() and len(s) == 8:
            try:
                dt = datetime.strptime(s, "%Y%m%d").date()
                return dt.isoformat()
            except Exception:
                pass

        # Try dateutil (handles many human formats). Prefer dayfirst since many CSVs are DD/MM/YY.
        try:
            dt = dateutil_parser.parse(s, dayfirst=True, yearfirst=False)
            return dt.date().isoformat()
        except Exception:
            pass

        # Fallback to pandas parsing (infer formats)
        try:
            ts = pd.to_datetime(s, dayfirst=True, errors="coerce", infer_datetime_format=True)
            if not pd.isna(ts):
                return ts.date().isoformat()
        except Exception:
            pass

    except Exception:
        pass
    return None

def extract_trade_date_from_df(df: pd.DataFrame) -> str:
    """
    Inspect DataFrame for a date column and return ISO date string 'YYYY-MM-DD' if found, else ''.
    Strategy:
      1. Check common column names (case-insensitive).
      2. Try any column that looks date-like by sampling values.
      3. Scan first few cells as last resort.
    """
    if df is None or df.empty:
        return ""

    cols = [str(c).strip().upper() for c in df.columns]

    # 1) common column names
    for can in COMMON_DATE_COL_NAMES:
        if can in cols:
            ser = df.iloc[:, cols.index(can)].dropna().astype(str).str.strip()
            parsed = ser.apply(_parse_date_value).dropna()
            if not parsed.empty:
                return parsed.mode().iloc[0]

    # 2) try other columns by sampling up to first 30 non-null values
    for idx, col in enumerate(cols):
        ser = df.iloc[:, idx]
        sample_vals = ser.dropna().astype(str).str.strip().head(30)
        if sample_vals.empty:
            continue
        parsed = sample_vals.apply(_parse_date_value).dropna()
        if not parsed.empty:
            mode_date = parsed.mode().iloc[0]
            # accept if at least ~50% of parsed samples match the mode
            if (parsed == mode_date).sum() >= max(1, int(len(parsed) * 0.5)):
                return mode_date

    # 3) scan first few cells
    nrows = min(10, len(df))
    ncols = min(6, len(df.columns))
    candidates = []
    for r in range(nrows):
        for c in range(ncols):
            try:
                v = df.iat[r, c]
            except Exception:
                continue
            p = _parse_date_value(v)
            if p:
                candidates.append(p)
    if candidates:
        ser = pd.Series(candidates)
        return ser.mode().iloc[0]

    return ""
def extract_trade_date_from_filename(fname: str) -> str:
    """
    Extract trade date from filename only (basename).
    Priority:
      1) 8-digit token -> try YYYYMMDD then DDMMYYYY
      2) 6-digit token -> interpret as DDMMYY (preferred)
    Returns ISO date string 'YYYY-MM-DD' or empty string if none found/invalid.
    """
    base = os.path.basename(fname)
    # 1) 8-digit: prefer YYYYMMDD, then DDMMYYYY
    m8 = re.search(r'(\d{8})', base)
    if m8:
        token = m8.group(1)
        for fmt in ("%Y%m%d", "%d%m%Y"):
            try:
                dt = datetime.strptime(token, fmt).date()
                return dt.isoformat()
            except Exception:
                continue

    # 2) 6-digit: interpret as DDMMYY (explicitly)
    #    Use pivot relative to current year to expand 2-digit year -> 20xx (preferred)
    m6 = re.search(r'(\d{6})', base)
    if m6:
        token = m6.group(1)
        dd = int(token[0:2])
        mm = int(token[2:4])
        yy = int(token[4:6])

        # Pivot: if yy <= (current_year%100 + 5) => 2000+yy else 1900+yy
        cur_year = datetime.utcnow().year
        pivot = (cur_year % 100) + 5
        yyyy = 2000 + yy if yy <= pivot else 1900 + yy

        try:
            # Interpret as DDMMYY => day=dd, month=mm
            dt = datetime(yyyy, mm, dd).date()
            return dt.isoformat()
        except Exception:
            # As a defensive fallback attempt MMDDYY if the above failed (rare)
            try:
                dt = datetime(yyyy, dd, mm).date()
                return dt.isoformat()
            except Exception:
                pass

    return ""

# ---------------------------
# End date helpers
# ---------------------------



# ---------------------------
# The functions to handle the data
# ---------------------------

# 1) configload() -> returns config dict
def configload(config_path: str = None) -> dict:
    cfg = load_config(config_path)
    logger.info("Config loaded")
    return cfg

# 2) candidatelistgeneration() -> returns pandas.DataFrame of candidates and diagnostics list
def candidatelistgeneration(zip_path: str, cfg: dict) -> Tuple[pd.DataFrame, List[str]]:
    diagnostics = []
    with zipfile.ZipFile(zip_path, 'r') as z:
        found_files = find_files_in_zip(zip_path)
        candidates = {}

        # HL (circuit) first
        for fname in found_files["HL"]:
            try:
                df = read_csv_from_zip(z, fname)
            except Exception as e:
                diagnostics.append(f"HL read error {fname}: {e}")
                continue
            sec_col = cfg["mappings"]["HL"]["security"]
            if sec_col not in df.columns:
                diagnostics.append(f"HL {fname} missing column {sec_col}")
                continue

            # <-- get date from filename only -->
            trade_date = extract_trade_date_from_filename(os.path.basename(fname))

            status_col = cfg["mappings"]["HL"].get("status")
            status_col = status_col if status_col in df.columns else None
            for _, row in df.iterrows():
                sym = normalize_symbol(row.get(sec_col))
                if not sym:
                    continue
                entry = candidates.setdefault(sym, {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "reasons": set(),
                    "pct": None,
                    "last_price": None,
                    "prev_close": None,
                    "turnover": None
                })
                entry["reasons"].add("circuit")
                if status_col:
                    entry["status"] = row.get(status_col)

        # GL (gainers/losers)
        for fname in found_files["GL"]:
            try:
                df = read_csv_from_zip(z, fname)
            except Exception as e:
                diagnostics.append(f"GL read error {fname}: {e}")
                continue
            sec_col = cfg["mappings"]["GL"]["security"]
            if sec_col not in df.columns:
                diagnostics.append(f"GL {fname} missing column {sec_col}")
                continue

            # <-- get date from filename only -->
            trade_date = extract_trade_date_from_filename(os.path.basename(fname))

            pct_col = safe_first_existing_col(df, cfg["mappings"]["GL"]["pct"])
            close_col = safe_first_existing_col(df, cfg["mappings"]["GL"]["close"])
            prev_col = cfg["mappings"]["GL"]["prev_close"]
            if prev_col and prev_col not in df.columns:
                prev_col = None
            if pct_col:
                df["_PCT"] = df[pct_col].apply(lambda x: to_float_safe(x))
                df["_ABS_PCT"] = df["_PCT"].abs()
                sel = df[df["_ABS_PCT"] >= cfg["thresholds"]["candidate_pct_threshold"]]
                topn = df.sort_values("_ABS_PCT", ascending=False).head(cfg["thresholds"]["candidate_top_n"])
                sel = pd.concat([sel, topn]).drop_duplicates(subset=[sec_col])
            else:
                sel = df.head(cfg["thresholds"]["candidate_top_n"])
            for _, row in sel.iterrows():
                sym = normalize_symbol(row.get(sec_col))
                if not sym:
                    continue
                pct = to_float_safe(row.get(pct_col)) if pct_col else None
                last_price = to_float_safe(row.get(close_col)) if close_col else None
                prev = to_float_safe(row.get(prev_col)) if prev_col else None
                entry = candidates.setdefault(sym, {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "reasons": set(),
                    "pct": None,
                    "last_price": None,
                    "prev_close": None,
                    "turnover": None
                })
                entry["reasons"].add("gainer_loser")
                entry["pct"] = pct if pct is not None else entry["pct"]
                entry["last_price"] = last_price if last_price is not None else entry["last_price"]
                entry["prev_close"] = prev if prev is not None else entry["prev_close"]

        # TT (top turnover)
        for fname in found_files["TT"]:
            try:
                df = read_csv_from_zip(z, fname)
            except Exception as e:
                diagnostics.append(f"TT read error {fname}: {e}")
                continue
            sec_col = cfg["mappings"]["TT"]["security"]
            if sec_col not in df.columns:
                diagnostics.append(f"TT {fname} missing column {sec_col}")
                continue

            # <-- get date from filename only -->
            trade_date = extract_trade_date_from_filename(os.path.basename(fname))

            val_col = safe_first_existing_col(df, cfg["mappings"]["TT"]["net_trdval"])
            qty_col = safe_first_existing_col(df, cfg["mappings"]["TT"]["net_trdqty"])
            if val_col:
                df["_VAL"] = df[val_col].apply(to_float_safe)
                topn = df.sort_values("_VAL", ascending=False).head(cfg["thresholds"]["tt_top_n"])
            elif qty_col:
                df["_QTY"] = df[qty_col].apply(to_float_safe)
                topn = df.sort_values("_QTY", ascending=False).head(cfg["thresholds"]["tt_top_n"])
            else:
                topn = df.head(cfg["thresholds"]["tt_top_n"])

            for _, row in topn.iterrows():
                sym = normalize_symbol(row.get(sec_col))
                if not sym:
                    continue
                entry = candidates.setdefault(sym, {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "reasons": set(),
                    "pct": None,
                    "last_price": None,
                    "prev_close": None,
                    "turnover": None
                })
                entry["reasons"].add("top_turnover")
                entry["turnover"] = to_float_safe(row.get(val_col)) if val_col else entry["turnover"]

    # Build DataFrame
    rows = []
    for sym, v in candidates.items():
        rows.append({
            "trade_date": v.get("trade_date"),
            "symbol": sym,
            "reason_set": ",".join(sorted(v.get("reasons") or [])),
            "primary_reason": sorted(v.get("reasons") or [])[0] if v.get("reasons") else None,
            "pct_change": v.get("pct"),
            "prev_close": v.get("prev_close"),
            "last_price": v.get("last_price"),
            "turnover": v.get("turnover"),
            "notes": json.dumps({k: v.get(k) for k in ("status",) if v.get(k) is not None})
        })
    df_cand = pd.DataFrame(rows, columns=["trade_date", "symbol", "reason_set", "primary_reason", "pct_change", "prev_close", "last_price", "turnover", "notes"])
    logger.info("Candidate generation produced %d symbols", len(df_cand))
    return df_cand, diagnostics


# 3) writecandidatelisttocsv()
def writecandidatelisttocsv(df: pd.DataFrame, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True) if os.path.dirname(out_path) else None
    df.to_csv(out_path, index=False)
    logger.info("Wrote candidates CSV: %s (%d rows)", out_path, len(df))
    return out_path

# 4) Updatecandidatelistcsv() -> enrich the CSV from PR and optionally update gainer_loser and circuit_hitter tables
def Updatecandidatelistcsv(zip_path: str, candidates_csv: str, cfg: dict, update_aux_tables: bool = True) -> Tuple[str, List[str]]:
    diagnostics = []
    df_cand = pd.read_csv(candidates_csv)
    pr_map = {}
    with zipfile.ZipFile(zip_path, 'r') as z:
        found_files = find_files_in_zip(zip_path)
        pr_files = found_files.get("PR", [])
        for pr_file in pr_files:
            try:
                pr_df = read_csv_from_zip(z, pr_file)
            except Exception as e:
                diagnostics.append(f"PR read error {pr_file}: {e}")
                continue

            # <-- determine trade date from PR filename only -->
            trade_date = extract_trade_date_from_filename(os.path.basename(pr_file))

            security_col = cfg["mappings"]["PR"]["security"]
            if security_col not in pr_df.columns:
                diagnostics.append(f"PR file {pr_file} missing {security_col}")
                continue
            for _, row in pr_df.iterrows():
                sym = normalize_symbol(row.get(security_col))
                if not sym:
                    continue
                rec = row.to_dict()
                # attach the filename-derived trade date for later merging
                rec["TRADE_DATE_FROM_FILENAME"] = trade_date
                pr_map.setdefault(sym, rec)

        # merge PR info into candidates (enriched)
        enriched_rows = []
        for _, candidate_row in df_cand.iterrows():
            sym = normalize_symbol(candidate_row["symbol"])
            merged_record = candidate_row.to_dict()
            pr_row = pr_map.get(sym)
            if pr_row:
                pr_map_cols = cfg["mappings"]["PR"]
                for mapping_key, mapping_col in pr_map_cols.items():
                    if isinstance(mapping_col, list):
                        for c in mapping_col:
                            if c in pr_row:
                                merged_record[mapping_key] = pr_row.get(c)
                                break
                    else:
                        merged_record[mapping_key] = pr_row.get(mapping_col) if mapping_col in pr_row else None
                merged_record["pr_source_file"] = pr_file

                # <-- force trade_date from PR filename if present -->
                if pr_row.get("TRADE_DATE_FROM_FILENAME"):
                    merged_record["trade_date"] = pr_row.get("TRADE_DATE_FROM_FILENAME")
            else:
                diagnostics.append(f"PR record not found for candidate {sym}")
            enriched_rows.append(merged_record)

        enriched_df = pd.DataFrame(enriched_rows)
        enriched_csv = candidates_csv.replace(".csv", ".enriched.csv")
        os.makedirs(os.path.dirname(enriched_csv), exist_ok=True) if os.path.dirname(enriched_csv) else None
        enriched_df.to_csv(enriched_csv, index=False)
        logger.info("Created enriched CSV: %s (%d rows)", enriched_csv, len(enriched_df))

    # (rest of Updatecandidatelistcsv unchanged: upsert audit tables)
    if update_aux_tables:
        engine = connect_db(cfg["db"])
        try:
            _upsert_gainer_loser_table(engine, df_cand, is_dry_run=cfg["behavior"].get("dry_run", True))
            _upsert_circuit_hitter_table(engine, df_cand, is_dry_run=cfg["behavior"].get("dry_run", True))

            matched_pr_count = sum(1 for r in enriched_rows if r.get("pr_source_file"))
            try:
                processed_files_list = find_files_in_zip(zip_path)["PR"] + find_files_in_zip(zip_path)["GL"] + find_files_in_zip(zip_path)["HL"] + find_files_in_zip(zip_path)["TT"]
            except Exception:
                processed_files_list = []

            extracted_trade_date = enriched_rows[0].get("trade_date") if enriched_rows else None
            _write_ingest_audit_row(
                engine=engine,
                zip_filename=os.path.basename(zip_path),
                trade_date=extracted_trade_date,
                processed_file_list=processed_files_list,
                candidate_count=len(df_cand),
                matched_pr_count=matched_pr_count,
                warnings_list=diagnostics,
                is_dry_run=cfg["behavior"].get("dry_run", True)
            )
        finally:
            try:
                engine.dispose()
            except Exception:
                pass

    return enriched_csv, diagnostics

# ---------------------------
# Audit upsert helpers (clear variable names)
# ---------------------------

def _upsert_gainer_loser_table(engine, candidate_dataframe: pd.DataFrame, is_dry_run: bool = True):
    """
    Upsert gainer_loser audit table. Replaces NaN with None before DB call.
    Expects candidate_dataframe columns: trade_date, symbol, pct_change, prev_close, last_price, reason_set, notes
    """
    ddl_statement = text("""
    CREATE TABLE IF NOT EXISTS gainer_loser (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE,
        symbol VARCHAR(128) NOT NULL,
        pct_change DECIMAL(9,4),
        prev_close DECIMAL(18,4),
        last_price DECIMAL(18,4),
        reason_set VARCHAR(255),
        notes JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NULL,
        UNIQUE KEY ux_gl_symbol_date (symbol, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    upsert_statement = text("""
    INSERT INTO gainer_loser
        (trade_date, symbol, pct_change, prev_close, last_price, reason_set, notes)
    VALUES
        (:trade_date, :symbol, :pct_change, :prev_close, :last_price, :reason_set, :notes)
    ON DUPLICATE KEY UPDATE
        pct_change = VALUES(pct_change),
        prev_close = VALUES(prev_close),
        last_price = VALUES(last_price),
        reason_set = VALUES(reason_set),
        notes = VALUES(notes),
        updated_at = NOW();
    """)

    records = []
    for _, rec in candidate_dataframe.iterrows():
        # sanitize numeric fields so we never pass NaN to pymysql
        pct = none_if_nan(rec.get("pct_change"))
        prev = none_if_nan(rec.get("prev_close"))
        last = none_if_nan(rec.get("last_price"))

        # notes: ensure it's a JSON string or None
        raw_notes = rec.get("notes")
        notes_json = None
        if isinstance(raw_notes, str) and raw_notes.strip().startswith(('{', '[')):
            # validate strict JSON
            try:
                json.loads(raw_notes)
                notes_json = raw_notes
            except Exception:
                notes_json = json.dumps({"raw_notes": raw_notes}, ensure_ascii=False)
        elif raw_notes is not None and not pd.isna(raw_notes):
            notes_json = json.dumps({"notes": str(raw_notes)}, ensure_ascii=False)

        records.append({
            "trade_date": rec.get("trade_date"),
            "symbol": rec.get("symbol"),
            "pct_change": pct,
            "prev_close": prev,
            "last_price": last,
            "reason_set": rec.get("reason_set"),
            "notes": notes_json
        })

    if not records:
        logger.info("No gainer_loser records to upsert.")
        return

    if is_dry_run:
        logger.info("[DRY RUN] Preparing to upsert %d rows into gainer_loser table.", len(records))
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                conn.execute(ddl_statement)
                for i, param in enumerate(records, start=1):
                    logger.info("[DRY RUN] gainer_loser upsert %d/%d: %s", i, len(records), {"symbol": param["symbol"], "trade_date": param["trade_date"]})
                    conn.execute(upsert_statement, [param])
                tx.rollback()
                logger.info("[DRY RUN] Rolled back gainer_loser dry-run.")
            except Exception:
                try:
                    tx.rollback()
                except Exception:
                    pass
                logger.exception("[DRY RUN] Error during gainer_loser dry-run; rolled back.")
                raise
        return

    # production path - commit in batches
    batch_size = 500
    with engine.begin() as conn:
        conn.execute(ddl_statement)
        for start in range(0, len(records), batch_size):
            batch = records[start:start+batch_size]
            conn.execute(upsert_statement, batch)
    logger.info("Upserted %d records into gainer_loser table.", len(records))

def _upsert_circuit_hitter_table(engine, candidate_dataframe: pd.DataFrame, is_dry_run: bool = True):
    """
    Upsert circuit_hitter audit table. Converts NaN to None and ensures notes are valid JSON or NULL.
    Expects candidate_dataframe columns: trade_date, symbol, status, reason_set, notes
    """
    ddl_statement = text("""
    CREATE TABLE IF NOT EXISTS circuit_hitter (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE,
        symbol VARCHAR(128) NOT NULL,
        status VARCHAR(32),
        reason_set VARCHAR(255),
        notes JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NULL,
        UNIQUE KEY ux_ch_symbol_date (symbol, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    upsert_statement = text("""
    INSERT INTO circuit_hitter
        (trade_date, symbol, status, reason_set, notes)
    VALUES
        (:trade_date, :symbol, :status, :reason_set, :notes)
    ON DUPLICATE KEY UPDATE
        status = VALUES(status),
        reason_set = VALUES(reason_set),
        notes = VALUES(notes),
        updated_at = NOW();
    """)

    records = []
    for _, rec in candidate_dataframe.iterrows():
        raw_notes = rec.get("notes")
        notes_json = None
        if isinstance(raw_notes, str) and raw_notes.strip().startswith(('{', '[')):
            try:
                json.loads(raw_notes)
                notes_json = raw_notes
            except Exception:
                notes_json = json.dumps({"raw_notes": raw_notes}, ensure_ascii=False)
        elif raw_notes is not None and not pd.isna(raw_notes):
            notes_json = json.dumps({"notes": str(raw_notes)}, ensure_ascii=False)

        status_val = rec.get("status") if ("status" in rec.index and not pd.isna(rec.get("status"))) else None

        records.append({
            "trade_date": rec.get("trade_date"),
            "symbol": rec.get("symbol"),
            "status": status_val,
            "reason_set": rec.get("reason_set"),
            "notes": notes_json
        })

    if not records:
        logger.info("No circuit_hitter records to upsert.")
        return

    if is_dry_run:
        logger.info("[DRY RUN] Preparing to upsert %d rows into circuit_hitter table.", len(records))
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                conn.execute(ddl_statement)
                for i, param in enumerate(records, start=1):
                    logger.info("[DRY RUN] Upserting circuit_hitter %d/%d: %s", i, len(records), {"symbol": param["symbol"], "trade_date": param["trade_date"]})
                    conn.execute(upsert_statement, [param])
                tx.rollback()
                logger.info("[DRY RUN] Rolled back circuit_hitter dry-run.")
            except Exception:
                try:
                    tx.rollback()
                except Exception:
                    pass
                logger.exception("[DRY RUN] Error during circuit_hitter dry-run; rolled back.")
                raise
        return

    # production commit path
    batch_size = 500
    with engine.begin() as conn:
        conn.execute(ddl_statement)
        for start in range(0, len(records), batch_size):
            batch = records[start:start+batch_size]
            conn.execute(upsert_statement, batch)
    logger.info("Upserted %d records into circuit_hitter table.", len(records))

# ingest_audit writer
def _write_ingest_audit_row(
    engine,
    zip_filename: str,
    trade_date: str | None,
    processed_file_list: list,
    candidate_count: int,
    matched_pr_count: int,
    warnings_list: list | None = None,
    is_dry_run: bool = True
):
    """
    Insert a single ingest/audit record into ingest_audit table.
    """
    ddl_create = text("""
    CREATE TABLE IF NOT EXISTS ingest_audit (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      zip_file VARCHAR(512) NOT NULL,
      trade_date DATE NULL,
      file_list JSON NULL,
      candidates_count INT DEFAULT 0,
      matched_pr_count INT DEFAULT 0,
      warnings TEXT NULL,
      started_at DATETIME NULL,
      finished_at DATETIME NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    insert_statement = text("""
    INSERT INTO ingest_audit
      (zip_file, trade_date, file_list, candidates_count, matched_pr_count, warnings, started_at, finished_at)
    VALUES
      (:zip_file, :trade_date, :file_list, :candidates_count, :matched_pr_count, :warnings, :started_at, :finished_at)
    """)

    start_ts = datetime.utcnow()
    finish_ts = datetime.utcnow()
    warnings_joined = "\n".join([str(w) for w in warnings_list]) if warnings_list else None

    try:
        file_list_json_text = json.dumps(processed_file_list, ensure_ascii=False)
        json.loads(file_list_json_text)
    except Exception:
        file_list_json_text = safe_json_dumps({"files": processed_file_list})

    params = {
        "zip_file": zip_filename,
        "trade_date": trade_date,
        "file_list": file_list_json_text,
        "candidates_count": int(candidate_count or 0),
        "matched_pr_count": int(matched_pr_count or 0),
        "warnings": warnings_joined,
        "started_at": start_ts,
        "finished_at": finish_ts
    }

    if is_dry_run:
        logger.info("[DRY RUN] Will write ingest audit for zip=%s trade_date=%s candidates=%d matched_pr=%d",
                    zip_filename, trade_date, candidate_count, matched_pr_count)
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(ddl_create)
                connection.execute(insert_statement, params)
                transaction.rollback()
                logger.info("[DRY RUN] Ingest audit simulated and rolled back.")
            except Exception as e:
                logger.exception("[DRY RUN] Error writing ingest audit (rolled back): %s", e)
                try:
                    transaction.rollback()
                except Exception:
                    pass
                raise
        return

    with engine.begin() as connection:
        connection.execute(ddl_create)
        connection.execute(insert_statement, params)
    logger.info("Wrote ingest audit: zip=%s trade_date=%s candidates=%d matched_pr=%d",
                zip_filename, trade_date, candidate_count, matched_pr_count)

# ---------------------------
# Main upsert to intraday_bhavcopy
# ---------------------------
def upsert_candidatelist(enriched_csv: str, cfg: dict) -> None:
    """
    Upsert enriched CSV rows into intraday_bhavcopy.
    - dry_run (cfg["behavior"]["dry_run"]) executes per-row with logging and rolls back.
    - production uses batched executemany (fast) and commits.
    """
    df = pd.read_csv(enriched_csv)
    engine = connect_db(cfg["db"])
    dry_run = cfg["behavior"].get("dry_run", True)

    insert_sql = text("""
    INSERT INTO intraday_bhavcopy
      (trade_date, symbol, mkt_flag, ind_sec, corp_ind, prev_close, open, high, low, close,
       net_trdval, net_trdqty, trades, hi_52_wk, lo_52_wk, extras, created_at)
    VALUES
      (:trade_date, :symbol, :mkt_flag, :ind_sec, :corp_ind, :prev_close, :open, :high, :low, :close,
       :net_trdval, :net_trdqty, :trades, :hi_52_wk, :lo_52_wk, :extras, NOW())
    ON DUPLICATE KEY UPDATE
      mkt_flag = VALUES(mkt_flag),
      ind_sec = VALUES(ind_sec),
      corp_ind = VALUES(corp_ind),
      prev_close = VALUES(prev_close),
      open = VALUES(open),
      high = VALUES(high),
      low = VALUES(low),
      close = VALUES(close),
      net_trdval = VALUES(net_trdval),
      net_trdqty = VALUES(net_trdqty),
      trades = VALUES(trades),
      hi_52_wk = VALUES(hi_52_wk),
      lo_52_wk = VALUES(lo_52_wk),
      extras = VALUES(extras),
      updated_at = NOW();
    """)

    rows_to_insert = []
    for _, row in df.iterrows():
        market_flag = 1 if str(row.get("mkt") or row.get("mkt_flag") or "").strip().upper() == "Y" else 0
        try:
            extras_json = build_extras_json_from_series(row)
        except Exception as e:
            logger.warning("Failed to build extras JSON for %s: %s. Using fallback stringified extras.", row.get("symbol"), e)
            extras_json = json.dumps({k: (None if row.get(k) is None else str(row.get(k))) for k in row.index if k not in {
                "trade_date","symbol","mkt","mkt_flag","prev_close","open","high","low","close",
                "net_trdval","net_trdqty","trades","ind_sec","corp_ind","hi_52_wk","lo_52_wk"
            }}, ensure_ascii=False)

        row_map = {
            "trade_date": row.get("trade_date"),
            "symbol": row.get("symbol"),
            "mkt_flag": market_flag,
            "ind_sec": row.get("ind_sec") if "ind_sec" in row else None,
            "corp_ind": row.get("corp_ind") if "corp_ind" in row else None,
            "prev_close": row.get("prev_close") if "prev_close" in row else None,
            "open": row.get("open") if "open" in row else None,
            "high": row.get("high") if "high" in row else None,
            "low": row.get("low") if "low" in row else None,
            "close": row.get("close") if "close" in row else None,
            "net_trdval": row.get("net_trdval") if "net_trdval" in row else None,
            "net_trdqty": row.get("net_trdqty") if "net_trdqty" in row else None,
            "trades": row.get("trades") if "trades" in row else None,
            "hi_52_wk": row.get("hi_52_wk") if "hi_52_wk" in row else None,
            "lo_52_wk": row.get("lo_52_wk") if "lo_52_wk" in row else None,
            "extras": extras_json
        }
        rows_to_insert.append(row_map)

    if not rows_to_insert:
        logger.info("No enriched rows to upsert into intraday_bhavcopy.")
        return

    if dry_run:
        logger.info("[DRY RUN] Starting manual transaction (no commit). Total rows: %d", len(rows_to_insert))
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                for idx, row_map in enumerate(rows_to_insert, start=1):
                    preview = {k: row_map[k] for k in ('symbol', 'trade_date', 'prev_close', 'open', 'high', 'low', 'close') if k in row_map}
                    logger.info("[DRY RUN] Inserting row %d/%d: %s", idx, len(rows_to_insert), preview)
                    conn.execute(insert_sql, [row_map])
                trans.rollback()
                logger.info("[DRY RUN] Completed dry-run inserts and rolled back.")
            except Exception as e:
                logger.exception("[DRY RUN] Error during dry-run inserts: %s", e)
                try:
                    trans.rollback()
                except Exception:
                    pass
                raise
        return

    # Production: batch insert
    batch_size = 500
    inserted_count = 0
    with engine.begin() as conn:
        for start in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[start:start + batch_size]
            conn.execute(insert_sql, batch)
            inserted_count += len(batch)
    logger.info("Upserted %d candidate rows into intraday_bhavcopy", inserted_count)

# ---------------------------
# CLI and orchestrator
# ---------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("zipfile", nargs="?", help="Path to daily ZIP (e.g., PR031025.zip)")
    p.add_argument("config", nargs="?", help="Path to config YAML/JSON (optional)", default=None)
    p.add_argument("--preview", action="store_true", help="Preview only (no DB writes) - alias for behavior.preview_only")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    CONFIG_PATH = args.config

    cfg = configload(CONFIG_PATH)
    logger.info("Config loaded")

    output_root = cfg.get("paths", {}).get("output_root", "./Output/Intraday")
    output_root = os.path.normpath(output_root)

    if args.preview:
        cfg["behavior"]["preview_only"] = True
        cfg["behavior"]["dry_run"] = True

    zip_arg = args.zipfile
    if zip_arg:
        if os.path.isabs(zip_arg):
            ZIP_PATH = zip_arg
        else:
            ZIP_PATH = os.path.join(output_root, zip_arg)
    else:
        if not os.path.isdir(output_root):
            logger.error("Configured output_root does not exist or is not a directory: %s", output_root)
            sys.exit(1)
        zips = sorted(
            [f for f in os.listdir(output_root) if f.lower().endswith(".zip")],
            key=lambda f: os.path.getmtime(os.path.join(output_root, f)),
            reverse=True,
        )
        if not zips:
            logger.error("No ZIP files found in output_root: %s", output_root)
            sys.exit(1)
        ZIP_PATH = os.path.join(output_root, zips[0])
        ZIP_PATH = os.path.normpath(ZIP_PATH)
        logger.info("No ZIP argument passed — using latest ZIP: %s", ZIP_PATH.replace("\\", "/"))

    ZIP_PATH = os.path.normpath(ZIP_PATH)
    if not os.path.exists(ZIP_PATH):
        logger.error("ZIP file not found: %s", ZIP_PATH)
        sys.exit(1)

    # Pipeline steps
    candidate_df, diagnostics = candidatelistgeneration(ZIP_PATH, cfg)
    for diag in diagnostics:
        logger.warning("Diag: %s", diag)

    out_csv_path = os.path.join(output_root, cfg["output"].get("candidates_csv", "candidates_preview.csv"))
    writecandidatelisttocsv(candidate_df, out_csv_path)

    enriched_csv_path, enrichment_diagnostics = Updatecandidatelistcsv(ZIP_PATH, out_csv_path, cfg, update_aux_tables=True)
    for d in enrichment_diagnostics:
        logger.warning("Update diag: %s", d)

    if not args.preview and not cfg["behavior"].get("preview_only"):
        upsert_candidatelist(enriched_csv_path, cfg)
    else:
        logger.info("Preview mode: skipping DB upsert. Enriched CSV at %s", enriched_csv_path)

    logger.info("Pipeline finished.")
