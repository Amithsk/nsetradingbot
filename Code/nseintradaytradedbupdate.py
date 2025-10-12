#!/usr/bin/env python3
"""
cand_pipeline_modular.py

Modular candidate pipeline with single-responsibility functions:
- configload()
- candidatelistgeneration()
- writecandidatelisttocsv()
- Updatecandidatelistcsv()
- upsert_candidatelist()

Also: helper functions to update circuit_hitter and gainer_loser tables.

Usage:
  python cand_pipeline_modular.py /path/to/PR031025.zip config.yaml --preview

Requirements:
  pip install pandas pyyaml pymysql
"""

import os
import urllib.parse
from sqlalchemy import create_engine,text
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
from typing import Tuple, Dict, List, Any
import numpy as np
import pandas as pd


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
        "preview_only": False,
        "dry_run": True # if True, DB writes are rolled back,If False, DB writes are committed
    },
    "output": {
        "candidates_csv": "candidates_preview.csv"
    },
  
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
        # shallow merge for top-level keys
        for k, v in cfg.items():
            if isinstance(v, dict) and k in merged:
                merged[k].update(v)
            else:
                merged[k] = v
    return merged

def connect_db(db_cfg: dict = None):
    """
    Create and return a SQLAlchemy engine using environment variable MYSQL_PASSWORD.
    If db_cfg is provided, it overrides environment variables.
    """
    # 1. Get DB credentials (from env or config)
    password = os.getenv('MYSQL_PASSWORD') or (db_cfg.get("password") if db_cfg else "")
    encoded_pw = urllib.parse.quote_plus(password)  # escape special characters safely

    user = db_cfg.get("user", "root") if db_cfg else "root"
    host = db_cfg.get("host", "localhost") if db_cfg else "localhost"
    port = db_cfg.get("port", 3306) if db_cfg else 3306
    dbname = db_cfg.get("db", "intradaytrading") if db_cfg else "intradaytrading"

    # 2. Build SQLAlchemy engine URL
    DATABASE_URL = f"mysql+pymysql://{user}:{encoded_pw}@{host}:{port}/{dbname}"

    # 3. Create the engine
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False  # Set True only if you want SQL logs for debugging
    )

    logger.info("SQLAlchemy engine created for DB: %s", dbname)
    return engine

def extract_trade_date_from_filename(fname: str) -> str:
    m = re.search(r'(\d{6})', fname)
    if not m:
        return ""
    s = m.group(1)
    dd, mm, yy = int(s[:2]), int(s[2:4]), int(s[4:6])
    yyyy = 2000 + yy if yy < 70 else 1900 + yy
    try:
        return datetime(yyyy, mm, dd).date().isoformat()
    except Exception:
        return ""

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

def _make_json_serializable(value):
    """Normalize pandas / numpy / python values into JSON-safe values."""
    if value is None:
        return None
    # numpy integers/floats
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    s = str(value).strip()
    if s == "" or s.lower() in ("nan","none","null","n/a"):
        return None
    # try numeric conversion from strings
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

def build_extras_json_from_series(r):
    skip = {"trade_date","symbol","mkt","mkt_flag","prev_close","open","high","low","close",
            "net_trdval","net_trdqty","trades","ind_sec","corp_ind","hi_52_wk","lo_52_wk"}
    extras = {}
    for k in r.index:
        if k in skip:
            continue
        extras[k] = _make_json_serializable(r.get(k))
    # produce strict JSON text
    extras_json = json.dumps(extras, ensure_ascii=False)
    # sanity-check
    json.loads(extras_json)
    return extras_json

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
        found = find_files_in_zip(zip_path)
        candidates = {}
        # HL (circuit) first
        for fname in found["HL"]:
            try:
                df = read_csv_from_zip(z, fname)
            except Exception as e:
                diagnostics.append(f"HL read error {fname}: {e}")
                continue
            sec_col = cfg["mappings"]["HL"]["security"]
            if sec_col not in df.columns:
                diagnostics.append(f"HL {fname} missing column {sec_col}")
                continue
            status_col = cfg["mappings"]["HL"].get("status")
            status_col = status_col if status_col in df.columns else None
            for _, row in df.iterrows():
                sym = normalize_symbol(row.get(sec_col))
                if not sym:
                    continue
                trade_date = extract_trade_date_from_filename(os.path.basename(fname))
                entry = candidates.setdefault(sym, {"symbol": sym, "trade_date": trade_date, "reasons": set(), "pct": None, "last_price": None, "prev_close": None, "turnover": None})
                entry["reasons"].add("circuit")
                if status_col:
                    entry["status"] = row.get(status_col)

        # GL (gainers/losers)
        for fname in found["GL"]:
            try:
                df = read_csv_from_zip(z, fname)
            except Exception as e:
                diagnostics.append(f"GL read error {fname}: {e}")
                continue
            sec_col = cfg["mappings"]["GL"]["security"]
            if sec_col not in df.columns:
                diagnostics.append(f"GL {fname} missing column {sec_col}")
                continue
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
                trade_date = extract_trade_date_from_filename(os.path.basename(fname))
                pct = to_float_safe(row.get(pct_col)) if pct_col else None
                last_price = to_float_safe(row.get(close_col)) if close_col else None
                prev = to_float_safe(row.get(prev_col)) if prev_col else None
                entry = candidates.setdefault(sym, {"symbol": sym, "trade_date": trade_date, "reasons": set(), "pct": None, "last_price": None, "prev_close": None, "turnover": None})
                entry["reasons"].add("gainer_loser")
                entry["pct"] = pct if pct is not None else entry["pct"]
                entry["last_price"] = last_price if last_price is not None else entry["last_price"]
                entry["prev_close"] = prev if prev is not None else entry["prev_close"]

        # TT (top turnover)
        for fname in found["TT"]:
            try:
                df = read_csv_from_zip(z, fname)
            except Exception as e:
                diagnostics.append(f"TT read error {fname}: {e}")
                continue
            sec_col = cfg["mappings"]["TT"]["security"]
            if sec_col not in df.columns:
                diagnostics.append(f"TT {fname} missing column {sec_col}")
                continue
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
                trade_date = extract_trade_date_from_filename(os.path.basename(fname))
                entry = candidates.setdefault(sym, {"symbol": sym, "trade_date": trade_date, "reasons": set(), "pct": None, "last_price": None, "prev_close": None, "turnover": None})
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
    df_cand = pd.DataFrame(rows, columns=["trade_date","symbol","reason_set","primary_reason","pct_change","prev_close","last_price","turnover","notes"])
    logger.info("Candidate generation produced %d symbols", len(df_cand))
    return df_cand, diagnostics

# 3) writecandidatelisttocsv()
def writecandidatelisttocsv(df: pd.DataFrame, out_path: str) -> str:
    df.to_csv(out_path, index=False)
    logger.info("Wrote candidates CSV: %s (%d rows)", out_path, len(df))
    return out_path

# 4) Updatecandidatelistcsv() -> enrich the CSV from PR and optionally update gainer_loser and circuit_hitter tables
def Updatecandidatelistcsv(zip_path: str, candidates_csv: str, cfg: dict, update_aux_tables: bool = True) -> Tuple[str, List[str]]:
    """
    - reads candidates_csv
    - looks up PR files in zip and merges PR fields into CSV (creates enriched CSV)
    - optionally upserts raw rows into gainer_loser and circuit_hitter tables for audit
    Returns enriched_csv_path and diagnostics
    """
    diagnostics = []
    df_cand = pd.read_csv(candidates_csv)
    pr_map = {}
    with zipfile.ZipFile(zip_path, 'r') as z:
        found = find_files_in_zip(zip_path)
        pr_files = found.get("PR", [])
        for pr in pr_files:
            try:
                pr_df = read_csv_from_zip(z, pr)
            except Exception as e:
                diagnostics.append(f"PR read error {pr}: {e}")
                continue
            security_col = cfg["mappings"]["PR"]["security"]
            if security_col not in pr_df.columns:
                diagnostics.append(f"PR file {pr} missing {security_col}")
                continue
            for _, r in pr_df.iterrows():
                sym = normalize_symbol(r.get(security_col))
                if not sym:
                    continue
                # store the raw row dict
                pr_map.setdefault(sym, r.to_dict())

        # merge PR info
        enriched_rows = []
        for _, r in df_cand.iterrows():
            sym = normalize_symbol(r["symbol"])
            merged = r.to_dict()
            pr_row = pr_map.get(sym)
            if pr_row:
                # map PR columns into merged (use mapping keys)
                pr_map_cols = cfg["mappings"]["PR"]
                for key, col in pr_map_cols.items():
                    if isinstance(col, list):
                        for c in col:
                            if c in pr_row:
                                merged[key] = pr_row.get(c)
                                break
                    else:
                        merged[key] = pr_row.get(col) if col in pr_row else None
                merged["pr_source_file"] = pr  # last matched PR file
            else:
                diagnostics.append(f"PR record not found for candidate {sym}")
            enriched_rows.append(merged)

        enriched_df = pd.DataFrame(enriched_rows)
        enriched_csv = candidates_csv.replace(".csv", ".enriched.csv")
        enriched_df.to_csv(enriched_csv, index=False)
        logger.info("Created enriched CSV: %s (%d rows)", enriched_csv, len(enriched_df))

        # optionally update raw audit tables (gainer_loser / circuit_hitter)
        #if update_aux_tables:
        #    conn = connect_db(cfg["db"])
        #    try:
        #        _upsert_gainer_loser_from_candidates(conn, df_cand, cfg)
        #        _upsert_circuit_hitter_from_candidates(conn, df_cand, cfg)
        #    finally:
        #        conn.close()

    return enriched_csv, diagnostics

# helper: _upsert_gainer_loser_from_candidates
def _upsert_gainer_loser_from_candidates(engine, df_cand: pd.DataFrame, cfg: dict):
    ddl = text("""
    CREATE TABLE IF NOT EXISTS gainer_loser (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      trade_date DATE,
      symbol VARCHAR(64),
      pct_change DECIMAL(9,4),
      prev_close DECIMAL(18,4),
      last_price DECIMAL(18,4),
      reason_set VARCHAR(255),
      notes JSON,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY ux_symbol_date_reason (symbol, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    upsert_sql = text("""
    INSERT INTO gainer_loser (trade_date, symbol, pct_change, prev_close, last_price, reason_set, notes)
    VALUES (:trade_date, :symbol, :pct_change, :prev_close, :last_price, :reason_set, :notes)
    ON DUPLICATE KEY UPDATE
      pct_change = VALUES(pct_change),
      prev_close = VALUES(prev_close),
      last_price = VALUES(last_price),
      reason_set = VALUES(reason_set),
      notes = VALUES(notes),
      created_at = NOW();
    """)

    rows = [
        {
            "trade_date": r.get("trade_date"),
            "symbol": r.get("symbol"),
            "pct_change": r.get("pct_change"),
            "prev_close": r.get("prev_close"),
            "last_price": r.get("last_price"),
            "reason_set": r.get("reason_set"),
            "notes": r.get("notes")
        }
        for _, r in df_cand[df_cand["primary_reason"] == "gainer_loser"].iterrows()
    ]

    if not rows:
        logger.info("No gainer_loser candidates to upsert.")
        return

    with engine.begin() as conn:
        conn.execute(ddl)
        conn.execute(upsert_sql, rows)
    logger.info("Upserted %d rows into gainer_loser", len(rows))

# helper: _upsert_circuit_hitter_from_candidates
def _upsert_circuit_hitter_from_candidates(engine, df_cand: pd.DataFrame, cfg: dict):
    ddl = text("""
    CREATE TABLE IF NOT EXISTS circuit_hitter (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      trade_date DATE,
      symbol VARCHAR(64),
      status VARCHAR(64),
      reason_set VARCHAR(255),
      notes JSON,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY ux_symbol_date (symbol, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    upsert_sql = text("""
    INSERT INTO circuit_hitter (trade_date, symbol, status, reason_set, notes)
    VALUES (:trade_date, :symbol, :status, :reason_set, :notes)
    ON DUPLICATE KEY UPDATE
      status = VALUES(status),
      reason_set = VALUES(reason_set),
      notes = VALUES(notes),
      created_at = NOW();
    """)

    subset = df_cand[df_cand["reason_set"].str.contains("circuit", na=False)]
    rows = []
    for _, r in subset.iterrows():
        notes = r.get("notes") or "{}"
        try:
            status = json.loads(notes).get("status", None)
        except Exception:
            status = None
        rows.append({
            "trade_date": r.get("trade_date"),
            "symbol": r.get("symbol"),
            "status": status,
            "reason_set": r.get("reason_set"),
            "notes": notes
        })

    if not rows:
        logger.info("No circuit_hitter candidates to upsert.")
        return

    with engine.begin() as conn:
        conn.execute(ddl)
        conn.execute(upsert_sql, rows)
    logger.info("Upserted %d rows into circuit_hitter", len(rows))


# 5) upsert_candidatelist() -> upsert enriched CSV rows into intraday_bhavcopy
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

    # Prepare rows list with sanitized extras_json
    rows = []
    for _, r in df.iterrows():
        mkt_flag = 1 if str(r.get("mkt") or r.get("mkt_flag") or "").strip().upper() == "Y" else 0
        # build strict JSON for extras (using your helper)
        try:
            extras_json = build_extras_json_from_series(r)
        except Exception as e:
            # Fallback: stringify everything (safe but lossy)
            logger.warning("Failed to build extras JSON for %s: %s. Using fallback stringified extras.", r.get("symbol"), e)
            extras_json = json.dumps({k: (None if r.get(k) is None else str(r.get(k))) for k in r.index if k not in {
                "trade_date","symbol","mkt","mkt_flag","prev_close","open","high","low","close",
                "net_trdval","net_trdqty","trades","ind_sec","corp_ind","hi_52_wk","lo_52_wk"
            }}, ensure_ascii=False)

        row_map = {
            "trade_date": r.get("trade_date"),
            "symbol": r.get("symbol"),
            "mkt_flag": mkt_flag,
            "ind_sec": r.get("ind_sec") if "ind_sec" in r else None,
            "corp_ind": r.get("corp_ind") if "corp_ind" in r else None,
            "prev_close": r.get("prev_close") if "prev_close" in r else None,
            "open": r.get("open") if "open" in r else None,
            "high": r.get("high") if "high" in r else None,
            "low": r.get("low") if "low" in r else None,
            "close": r.get("close") if "close" in r else None,
            "net_trdval": r.get("net_trdval") if "net_trdval" in r else None,
            "net_trdqty": r.get("net_trdqty") if "net_trdqty" in r else None,
            "trades": r.get("trades") if "trades" in r else None,
            "hi_52_wk": r.get("hi_52_wk") if "hi_52_wk" in r else None,
            "lo_52_wk": r.get("lo_52_wk") if "lo_52_wk" in r else None,
            "extras": extras_json
        }
        rows.append(row_map)

    if not rows:
        logger.info("No enriched rows to upsert into intraday_bhavcopy.")
        return

    # Dry-run: insert one-by-one with verbose logging and guarantee rollback
    if dry_run:
        logger.info("[DRY RUN] Starting manual transaction (no commit). Total rows: %d", len(rows))
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                for i, row in enumerate(rows, start=1):
                    preview = {k: row[k] for k in ('symbol', 'trade_date', 'prev_close', 'open', 'high', 'low', 'close') if k in row}
                    logger.info("[DRY RUN] Inserting row %d/%d: %s", i, len(rows), preview)
                    # execute single row
                    conn.execute(insert_sql, [row])
                # done executing - rollback for safety
                logger.info("[DRY RUN] Executed %d rows — now rolling back.", len(rows))
                trans.rollback()
            except Exception:
                # rollback on error and re-raise (no double-rollback)
                try:
                    trans.rollback()
                except Exception:
                    pass
                logger.exception("[DRY RUN] Error during insert; transaction rolled back.")
                raise
        return

    # Production: batch insert (fast) with engine.begin() (auto commit)
    batch_size = 500
    inserted = 0
    with engine.begin() as conn:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            conn.execute(insert_sql, batch)
            inserted += len(batch)
    logger.info("Upserted %d candidate rows into intraday_bhavcopy", inserted)


# ---------------------------
# CLI and orchestrator
# ---------------------------

def parse_args():
    p = argparse.ArgumentParser()
    # make zipfile optional
    p.add_argument("zipfile", nargs="?", help="Path to daily ZIP (e.g., PR031025.zip)")
    p.add_argument("config", nargs="?", help="Path to config YAML/JSON (optional)", default=None)
    p.add_argument("--preview", action="store_true", help="Preview only (no DB writes)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    CONFIG_PATH = args.config

    # Step 0: load config
    cfg = configload(CONFIG_PATH)
    logger.info("Config loaded")

    # Resolve OUTPUT_ROOT
    output_root = cfg.get("paths", {}).get("output_root", "./Output/Intraday")
    # ensure output_root is normalized and exists
    output_root = os.path.normpath(output_root)

    # CLI zip argument (may be None)
    zip_arg = args.zipfile

    if zip_arg:
        # If user passed a zip path, accept absolute or relative (relative -> relative to output_root)
        if os.path.isabs(zip_arg):
            ZIP_PATH = zip_arg
        else:
            ZIP_PATH = os.path.join(output_root, zip_arg)
    else:
        # No zip argument passed: pick the most recent zip in output_root
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
        # normalize and log (show forward slashes)
        ZIP_PATH = os.path.normpath(ZIP_PATH)
        logger.info("No ZIP argument passed — using latest ZIP: %s", ZIP_PATH.replace("\\", "/"))

    # Final normalize and existence check (covers both branches)
    ZIP_PATH = os.path.normpath(ZIP_PATH)
    if not os.path.exists(ZIP_PATH):
        logger.error("ZIP file not found: %s", ZIP_PATH)
        sys.exit(1)

    # Now run pipeline steps (unchanged)
    cand_df, diag = candidatelistgeneration(ZIP_PATH, cfg)
    for d in diag:
        logger.warning("Diag: %s", d)

    out_csv = os.path.join(output_root, cfg["output"].get("candidates_csv", "candidates_preview.csv"))
    writecandidatelisttocsv(cand_df, out_csv)

    enriched_csv, upd_diag = Updatecandidatelistcsv(ZIP_PATH, out_csv, cfg, update_aux_tables=True)
    for d in upd_diag:
        logger.warning("Update diag: %s", d)

    if not args.preview and not cfg["behavior"].get("preview_only"):
        upsert_candidatelist(enriched_csv, cfg)
    else:
        logger.info("Preview mode: skipping DB upsert. Enriched CSV at %s", enriched_csv)

    logger.info("Pipeline finished.")