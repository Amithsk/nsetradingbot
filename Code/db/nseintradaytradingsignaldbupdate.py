import os
import sys
import json
import logging
import argparse
import urllib.parse
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError,IntegrityError
# --- Make sure project root is in sys.path so we can import from Code/*
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]  # go up from Code/db to project root (nsetradingbot)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from Code.utils.nseintradaytrading_utils import enrich_signals_with_stops_targets,compute_liquidity_metrics,apply_liquidity_filters
from Code.utils.nseintradaytradingdb_utils import find_offending_cells, sanitize_df_for_sql, safe_to_sql, merge_temp_to_target
from Code.utils.nseintraday_estimate_expected_hold_days import estimate_expected_hold_days




# -------------------------
# Default Config
# -------------------------
DEFAULT_CONFIG = {
    "db": {
        "host": "localhost",
        "user": "root",
        "password": "your_password",
        "db": "intradaytrading",
        "port": 3306
    },
    "behavior": {
        "preview_only": False,  # If True: compute features/signals but skip DB upserts (no CSVs are written)
        "dry_run": False         # If True: DB writes are executed inside transactions that are rolled back
    },
    "output": {
        # Kept for future debugging — not written by default
        "features_csv": "features_preview.csv",
        "signals_csv": "signals_preview.csv"
    },
    "liquidity_rules": {
        "hard_min_net_trdval": 50_000_000,   # 5 Crore
        "hard_min_net_trdqty": 100_000,
        "hard_min_trades": 1500,
        "min_price": 25,
        "ranking_weights": {"mom": 0.6, "turnover": 0.3, "vol": 0.1},
        "fno_boost": 1.15,
        "mode": "enforce"        #  enforce mode → drop failures / tag_only mode → keep all
}
    
}

# -------------------------
# Strategy Params (tunable)
# -------------------------
MOMENTUM_LOOKBACK_DAYS = 20
MOMENTUM_TOP_N = 20
MOMENTUM_EXPECTED_HOLD_DAYS = 5

GAP_MINIMUM_PERCENT = 2.0

VOL_BREAK_LOOKBACK_HIGH_DAYS = 20
ATR_LOOKBACK_DAYS = 14
ATR_STOP_MULTIPLIER = 1.5
ATR_TARGET_MULTIPLIER = 3.0
VOL_BREAK_EXPECTED_HOLD_DAYS = 10

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("signal_generator_configured")


# -------------------------
# DB connection helper
# -------------------------
def connect_db(db_cfg: dict = None):
    """
    Create and return a SQLAlchemy engine. db_cfg overrides environment variables if provided.

    Credentials: prefer MYSQL_PASSWORD env over config password
    """
    env_password = os.getenv("MYSQL_PASSWORD")
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


# -------------------------
# Config loader
# -------------------------
def load_config(config_path: str = None):
    cfg = DEFAULT_CONFIG.copy()
    if config_path:
        with open(config_path, "r") as fh:
            file_cfg = json.load(fh)
        # shallow merge: file overrides defaults per top-level key
        for k, v in file_cfg.items():
            if isinstance(v, dict) and k in cfg:
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg


# -------------------------
# Helpers: JSON sanitizer /Trade date
# -------------------------

def trading_day(engine_obj, date_iso: str = None) -> str | None:
    """
    Return an ISO date string representing a valid trading day (a date that exists in intraday_bhavcopy).

    Behavior:
      - If date_iso is provided:
         * If intraday_bhavcopy has rows for date_iso -> return date_iso
         * Else -> return the most recent trade_date < date_iso (or None if none)
      - If date_iso is None:
         * Return the MAX(trade_date) from intraday_bhavcopy (latest available trading day)
         * If intraday_bhavcopy is empty -> fallback to yesterday adjusted for weekend

    Returns:
        ISO date string like '2025-10-24', or None when no suitable date can be determined.
    """
    # 1) try to use DB info (preferred)
    try:
        # If user passed a date, check if that date has rows
        if date_iso:
            q = text("SELECT COUNT(1) AS c FROM intraday_bhavcopy WHERE trade_date = :d")
            df = pd.read_sql(q, engine_obj, params={"d": date_iso})
            if not df.empty and int(df["c"].iloc[0]) > 0:
                return pd.to_datetime(date_iso).date().isoformat()

            # find previous trading day strictly before date_iso
            q_prev = text("SELECT MAX(trade_date) AS prev FROM intraday_bhavcopy WHERE trade_date < :d")
            df_prev = pd.read_sql(q_prev, engine_obj, params={"d": date_iso})
            if not df_prev.empty and not pd.isna(df_prev["prev"].iloc[0]):
                return pd.to_datetime(df_prev["prev"].iloc[0]).date().isoformat()

            # no earlier trading day in DB
            return None

        # date_iso not provided -> return latest available trade_date
        q_latest = text("SELECT MAX(trade_date) AS last_date FROM intraday_bhavcopy")
        df_latest = pd.read_sql(q_latest, engine_obj)
        if not df_latest.empty and not pd.isna(df_latest["last_date"].iloc[0]):
            return pd.to_datetime(df_latest["last_date"].iloc[0]).date().isoformat()

    except Exception:
        # If DB is unreachable or query fails, fall through to local weekend fallback
        logger.exception("trading_day: DB lookup failed, falling back to local weekend-adjusted date")

    # 2) Fallback: use "yesterday" adjusted for weekend (local heuristic)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    if yesterday.weekday() == 5:  # Saturday -> use Friday
        yesterday -= timedelta(days=1)
    elif yesterday.weekday() == 6:  # Sunday -> use Friday
        yesterday -= timedelta(days=2)
    return yesterday.date().isoformat()

    

def sanitize_json_cell(val):
    """
    Convert a Python value into a JSON-text string suitable for inserting into a JSON column,
    or return None to represent SQL NULL.
    - dict/list/number/bool -> json.dumps(...)
    - string that is valid JSON -> re-dump canonical JSON
    - other string -> json.dumps(string) (wrap as JSON string)
    - None/NaN -> None
    """
    if val is None:
        return None
    # pandas NaN
    try:
        if isinstance(val, float) and np.isnan(val):
            return None
    except Exception:
        pass

    # numeric/bool/list/dict
    if isinstance(val, (dict, list, int, float, bool)):
        try:
            return json.dumps(val, default=str)
        except Exception:
            return json.dumps(str(val))

    # bytes -> decode
    if isinstance(val, (bytes, bytearray)):
        try:
            s = val.decode("utf-8", errors="ignore")
        except Exception:
            s = str(val)
        return json.dumps(s)

    # string path
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return None
        # if it looks like valid JSON, attempt to parse then re-dump for canonical JSON
        try:
            parsed = json.loads(s)
            return json.dumps(parsed, default=str)
        except Exception:
            # not valid JSON — treat as plain string and JSON-encode it
            return json.dumps(s)

    # fallback: stringify then json-encode
    try:
        return json.dumps(str(val))
    except Exception:
        return None


def sanitize_json_column(df, col_name):
    """
    Replace df[col_name] with sanitized JSON-text strings or None (SQL NULL).
    Returns the DataFrame of rows that were changed for logging (may be empty).
    """
    if col_name not in df.columns:
        return pd.DataFrame()

    changed_idx = []
    sanitized_values = []
    for idx, v in df[col_name].items():
        new_v = sanitize_json_cell(v)
        sanitized_values.append(new_v)
        # detect "changed" if original isn't None and new is different representation
        if v is None:
            if new_v is not None:
                changed_idx.append(idx)
        else:
            # basic heuristic: string vs non-string or mismatch after loads
            try:
                if isinstance(v, str):
                    try:
                        json.loads(v)
                    except Exception:
                        changed_idx.append(idx)
                else:
                    # non-string python objects were converted
                    changed_idx.append(idx)
            except Exception:
                changed_idx.append(idx)

    df[col_name] = pd.Series(sanitized_values, index=df.index)
    if changed_idx:
        logger.warning("Sanitized %d values in '%s' column; sample rows: %s",
                       len(changed_idx),
                       col_name,
                       df.loc[changed_idx[:8], ["symbol", "trade_date", col_name]].to_dict(orient="records"))
    return df.loc[changed_idx]


# -------------------------
# DB helpers (no filtering here — ingestion handles ETF/SME)
# -------------------------
def fetch_bhavcopy_range(engine_obj, start_date, end_date):
    sql = text("""
        SELECT symbol, trade_date, prev_close, open, high, low, close, net_trdqty, net_trdval
        FROM intraday_bhavcopy
        WHERE trade_date BETWEEN :start_date AND :end_date
        ORDER BY symbol, trade_date
    """)
    df = pd.read_sql(sql, engine_obj, params={"start_date": start_date, "end_date": end_date})
    if not df.empty:
        df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def upsert_features(engine_obj, df_features, dry_run=False):
    """
    Upsert strategy_features. If dry_run=True, rollback at the end.
    Uses engine_obj.begin() to manage the transaction safely.
    """
    if df_features is None or df_features.empty:
        logger.info("No features to upsert.")
        return

    if dry_run:
        logger.info("Dry-run enabled: would upsert %d feature rows (skipping DB writes).", len(df_features))
        logger.debug("Sample features (dry-run): %s", df_features.head(10).to_dict(orient="records"))
        return

    temp_table = "tmp_strategy_features_upsert"
    try:
        # engine.begin() returns a Connection that has an active transaction,
        # and commits/rolls-back automatically on exit.
        with engine_obj.begin() as conn:
            df = df_features.copy()
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            # write temp table using the Connection
            df.to_sql(temp_table, conn, index=False, if_exists="replace")

            insert_sql = f"""
            INSERT INTO strategy_features (trade_date, symbol, feature_name, value, created_at)
            SELECT trade_date, symbol, feature_name, value, NOW() FROM {temp_table}
            ON DUPLICATE KEY UPDATE value=VALUES(value), created_at=NOW()
            """
            conn.execute(text(insert_sql))

            # DROP temp table (separate statement)
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

        logger.info("Features upsert committed (%d rows).", len(df))
    except Exception as e:
        logger.exception("Features upsert failed: %s", e)
        # attempt cleanup outside transaction
        try:
            with engine_obj.connect() as cleanup_conn:
                cleanup_conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        except Exception:
            pass
        raise

def upsert_signals(engine_obj, df_signals, dry_run=False):
    """
    Upsert rows into strategy_signals.
    Ensures JSON columns 'params' and 'notes' are valid JSON text or NULL.
    Uses engine_obj.begin() to run merge and cleanup.
    """
    if df_signals is None or df_signals.empty:
        logger.info("No signals to upsert.")
        return

    if dry_run:
        logger.info("Dry-run enabled: would upsert %d signals (skipping DB writes).", len(df_signals))
        logger.debug("Sample signals (dry-run): %s", df_signals.head(10).to_dict(orient="records"))
        return

    # Work on a local copy
    df = df_signals.copy()
    # Normalize date column
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # Validate & align signal_type with sign of signal_score
    try:
        df['signal_score'] = pd.to_numeric(df['signal_score'], errors='coerce')
        df['expected_type'] = df['signal_score'].apply(lambda s: 'LONG' if pd.notna(s) and s >= 0 else 'SHORT')
        if 'signal_type' not in df.columns:
            df['signal_type'] = df['expected_type']
        else:
            df['signal_type'] = df['signal_type'].astype(str).str.upper().fillna('')
        mismatches = df[df['signal_type'] != df['expected_type']]
        if not mismatches.empty:
            logger.warning("Detected %d signal_type mismatches (auto-fixing). Sample: %s",
                           len(mismatches),
                           mismatches[['symbol', 'signal_score', 'signal_type', 'expected_type']].head(20).to_dict(orient='records'))
            df.loc[mismatches.index, 'signal_type'] = df.loc[mismatches.index, 'expected_type']
        df.drop(columns=['expected_type'], inplace=True, errors='ignore')
    except Exception:
        logger.exception("Signal validation guard failed — continuing without auto-fix.")

    # Ensure json/text columns exist
    if "params" in df.columns:
        # assume sanitize_json_column exists and mutates column to text/NULL
        try:
            sanitize_json_column(df, "params")
        except Exception:
            logger.exception("sanitize_json_column(params) failed; falling back to sanitize_df_for_sql later.")
    else:
        df["params"] = None

    if "notes" in df.columns:
        try:
            sanitize_json_column(df, "notes")
        except Exception:
            logger.exception("sanitize_json_column(notes) failed; falling back to sanitize_df_for_sql later.")
    else:
        df["notes"] = None

    # Log a sample
    try:
        sample_notes = df[["symbol", "trade_date", "notes"]].head(10).to_dict(orient="records")
        logger.info("Sample 'notes' values (post-sanitize): %s", sample_notes)
    except Exception:
        pass

    temp_table = "tmp_strategy_signals_upsert"

    # 1) Detect offenders
    bad = find_offending_cells(df) if 'find_offending_cells' in globals() else []
    if bad:
        logger.warning("Found offending non-primitive cells before to_sql. Sample: %s", bad[:10])
    else:
        logger.info("No offending non-primitive cells found (pre-sanitize).")

    # 2) Sanitize into a new DataFrame (do not mutate original unexpectedly)
    df_sanitized = sanitize_df_for_sql(df.copy()) if 'sanitize_df_for_sql' in globals() else df.copy()

    # 3) Validate sanitized result
    if not isinstance(df_sanitized, pd.DataFrame):
        logger.error("sanitize_df_for_sql returned unexpected type: %s", type(df_sanitized))
        raise TypeError("sanitize_df_for_sql must return a pandas.DataFrame")

    # 4) Write sanitized data to temp table (single write)
    safe_to_sql(df_sanitized, temp_table, engine_obj, if_exists="replace", index=False, chunksize=5000)

    # 5) Merge temp into final table using ON DUPLICATE KEY UPDATE (MySQL)
    insert_sql = f"""
        INSERT INTO strategy_signals
        (trade_date, strategy, version, params, symbol, signal_type, signal_score,
            entry_model, qty, entry_price, stop_price, target_price,
            expected_hold_days, notes, created_at)
        SELECT trade_date, strategy, version, params, symbol, signal_type, signal_score,
            entry_model, qty, entry_price, stop_price, target_price,
            expected_hold_days, notes, NOW()
        FROM {temp_table}
        ON DUPLICATE KEY UPDATE
            signal_type = VALUES(signal_type),
            signal_score = VALUES(signal_score),
            entry_model = VALUES(entry_model),
            qty = VALUES(qty),
            entry_price = COALESCE(VALUES(entry_price), strategy_signals.entry_price),
            stop_price  = COALESCE(VALUES(stop_price),  strategy_signals.stop_price),
            target_price = COALESCE(VALUES(target_price), strategy_signals.target_price),
            expected_hold_days = COALESCE(VALUES(expected_hold_days), strategy_signals.expected_hold_days),
            params = VALUES(params),
            notes = VALUES(notes),
            created_at = NOW();
    """

    # 6) Execute merge and cleanup
    try:
        with engine_obj.begin() as conn:
            conn.execute(text(insert_sql))
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        logger.info("Signals upsert committed (%d rows).", len(df_sanitized))
    except Exception as e:
        logger.exception("Signals upsert failed: %s", e)
        # attempt cleanup
        try:
            with engine_obj.begin() as cleanup_conn:
                cleanup_conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        except Exception:
            logger.exception("Failed to cleanup temp table after error.")
        raise

def record_run(engine_obj, run_name, run_params, run_summary=None, dry_run=False):
    """
    Insert strategy_runs row; honor dry_run by not persisting.
    Uses engine_obj.begin() so we don't try to begin() on a connection that already has a tx.
    """
    params_json = json.dumps(run_params)
    summary_json = json.dumps(run_summary) if run_summary is not None else None

    if dry_run:
        logger.info("Dry-run: skipping record_run for %s (would insert run metadata).", run_name)
        logger.debug("Run params (dry-run): %s; summary: %s", params_json, summary_json)
        return

    insert_sql = text("""
      INSERT INTO strategy_runs (strategy, params, run_name, started_at, finished_at, summary)
      VALUES (:strategy, :params, :run_name, :started_at, :finished_at, :summary)
    """)

    try:
        now = datetime.utcnow()
        # use engine.begin() to get a connection & transaction in one go
        with engine_obj.begin() as conn:
            conn.execute(insert_sql, {
                "strategy": "batch_signal_generation",
                "params": params_json,
                "run_name": run_name,
                "started_at": now,
                "finished_at": now,
                "summary": summary_json
            })
        logger.info("Recorded strategy_runs entry %s", run_name)
    except Exception as e:
        logger.exception("Recording strategy_runs failed: %s", e)
        raise
# -------------------------
# Feature computations
# -------------------------
def compute_daily_features_for_date(engine_obj, target_date, momentum_lookback=MOMENTUM_LOOKBACK_DAYS, atr_lookback=ATR_LOOKBACK_DAYS):
    maximum_history_days = max(momentum_lookback, atr_lookback) + 60
    start_date = (pd.to_datetime(target_date) - pd.Timedelta(days=int(maximum_history_days))).date()
    end_date = pd.to_datetime(target_date).date()

    bhav_df = fetch_bhavcopy_range(engine_obj, start_date.isoformat(), end_date.isoformat())
    if bhav_df.empty:
        logger.info("No bhavcopy rows found between %s and %s", start_date, end_date)
        return pd.DataFrame(columns=["trade_date", "symbol", "feature_name", "value"])

    bhav_df = bhav_df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    feature_rows = []

    for symbol, group in bhav_df.groupby("symbol"):
        g = group.reset_index(drop=True).copy()
        for col in ("close", "high", "low", "prev_close"):
            g[col] = pd.to_numeric(g[col], errors="coerce")

        if len(g) > momentum_lookback:
            g[f"momentum_{momentum_lookback}"] = g["close"].pct_change(periods=momentum_lookback) * 100.0

        tr1 = (g["high"] - g["low"]).abs().fillna(0)
        tr2 = (g["high"] - g["prev_close"]).abs().fillna(0)
        tr3 = (g["low"] - g["prev_close"]).abs().fillna(0)
        g["true_range"] = np.maximum.reduce([tr1.values, tr2.values, tr3.values])

        if len(g) >= atr_lookback:
            g[f"atr_{atr_lookback}"] = pd.Series(g["true_range"]).rolling(window=atr_lookback, min_periods=atr_lookback).mean().values

        delta = g["close"].diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        roll_up = up.rolling(14).mean()
        roll_down = down.rolling(14).mean().replace(0, np.nan)
        rs = roll_up / roll_down
        g["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

        row_on_date = g[g["trade_date"] == pd.to_datetime(target_date)]
        if row_on_date.empty:
            continue
        r = row_on_date.iloc[0]

        mom_col = f"momentum_{momentum_lookback}"
        atr_col = f"atr_{atr_lookback}"
        if mom_col in g.columns and not pd.isna(r.get(mom_col, np.nan)):
            feature_rows.append({"trade_date": target_date, "symbol": symbol, "feature_name": f"mom_{momentum_lookback}", "value": float(r[mom_col])})
        if atr_col in g.columns and not pd.isna(r.get(atr_col, np.nan)):
            feature_rows.append({"trade_date": target_date, "symbol": symbol, "feature_name": f"atr_{atr_lookback}", "value": float(r[atr_col])})
        if not pd.isna(r.get("rsi_14", np.nan)):
            feature_rows.append({"trade_date": target_date, "symbol": symbol, "feature_name": "rsi_14", "value": float(r["rsi_14"])})

    df_features = pd.DataFrame(feature_rows)
    logger.info("Computed %d features for %s", len(df_features), target_date)
    return df_features


# -------------------------
# Signal generation (unchanged logic)
# -------------------------
def generate_momentum_signals_for_date(engine_obj, target_date, lookback=MOMENTUM_LOOKBACK_DAYS, top_n=MOMENTUM_TOP_N, expected_hold_days=MOMENTUM_EXPECTED_HOLD_DAYS):
    feature_name = f"mom_{lookback}"
    sql = text("SELECT symbol, value FROM strategy_features WHERE feature_name = :feat AND trade_date = :d")
    df_mom = pd.read_sql(sql, engine_obj, params={"feat": feature_name, "d": target_date})
    if df_mom.empty:
        logger.info("No momentum features for %s", target_date)
        return pd.DataFrame()

    # pick top N by absolute magnitude, then preserve sign for side
    top_idx = df_mom['value'].abs().nlargest(top_n).index
    df_sorted = df_mom.loc[top_idx].copy()
    df_sorted = df_sorted.reindex(df_sorted['value'].abs().sort_values(ascending=False).index)

    rows = []
    params = {"lookback_days": lookback, "top_n": top_n, "selection": "abs_magnitude"}

    for _, r in df_sorted.iterrows():
        symbol = r["symbol"]
        score = float(r["value"])
        side = "LONG" if score >= 0 else "SHORT"

        # --- compute ATR for symbol/date (best-effort) ---
        atr_val = None
        try:
            atr_feat = f"atr_{ATR_LOOKBACK_DAYS}"
            q_atr = text(
                "SELECT value FROM strategy_features "
                "WHERE symbol = :sym AND feature_name = :feat AND trade_date = :d "
                "ORDER BY trade_date DESC LIMIT 1"
            )
            df_atr = pd.read_sql(q_atr, engine_obj, params={"sym": symbol, "feat": atr_feat, "d": target_date})
            if not df_atr.empty and pd.notna(df_atr.loc[0, "value"]):
                atr_val = float(df_atr.loc[0, "value"])
        except Exception:
            atr_val = None

        # --- compute expected hold days using helper (safe call) ---
        try:
            est_days, _ = estimate_expected_hold_days(
                engine=engine_obj,
                symbol=symbol,
                strategy="momentum_topn",
                score=score,
                atr_value=atr_val,
                lookback_days=252,
                pct_stop=0.005,
                pct_target=0.01,
                max_search_days=30,
                min_samples=8,
                atr_target_mult=ATR_TARGET_MULTIPLIER,
                avg_daily_move_factor=1.0,
                fallback_default=expected_hold_days,
                min_days=1,
                max_days=30,
                use_cache=True
            )
        except Exception:
            est_days = expected_hold_days

        # --- build row dict (no executable code inside dict) ---
        row = {
            "trade_date": target_date,
            "strategy": "momentum_topn",
            "version": "v1",
            "params": json.dumps(params),
            "symbol": symbol,
            "signal_type": side,
            "signal_score": score,
            "entry_model": "next_open",
            "qty": None,
            "entry_price": None,
            "stop_price": None,
            "target_price": None,
            "expected_hold_days": int(est_days),
            "notes": None
        }
        rows.append(row)

    logger.info("Generated %d momentum signals for %s", len(rows), target_date)
    return pd.DataFrame(rows)


def generate_gap_follow_signals_for_date(engine_obj, target_date, gap_threshold_percent=GAP_MINIMUM_PERCENT):
    sql = text("SELECT symbol, prev_close, open FROM intraday_bhavcopy WHERE trade_date = :d")
    df_open = pd.read_sql(sql, engine_obj, params={"d": target_date})
    if df_open.empty:
        logger.info("No bhavcopy rows for %s to detect gaps.", target_date)
        return pd.DataFrame()
    df_open = df_open.assign(gap_pct=((df_open["open"] - df_open["prev_close"]) / df_open["prev_close"]) * 100.0)
    df_filtered = df_open[df_open["gap_pct"].abs() >= gap_threshold_percent]
    rows = []
    params = {"gap_threshold_percent": gap_threshold_percent}
    for _, r in df_filtered.iterrows():
        side = "LONG" if r["gap_pct"] > 0 else "SHORT"
        rows.append({
            "trade_date": target_date,
            "strategy": "gap_follow",
            "version": "v1",
            "params": json.dumps(params),
            "symbol": r["symbol"],
            "signal_type": side,
            "signal_score": float(r["gap_pct"]),
            "entry_model": "open",
            "qty": None,
            "entry_price": float(r["open"]) if pd.notna(r["open"]) else None,
            "stop_price": None,
            "target_price": None,
            "expected_hold_days": 1,
            "notes": json.dumps({"gap_pct": float(r["gap_pct"])})
        })
    logger.info("Generated %d gap_follow signals for %s", len(rows), target_date)
    return pd.DataFrame(rows)


def generate_volatility_breakout_signals_for_date(engine_obj, target_date,
                                                  lookback_high_days=VOL_BREAK_LOOKBACK_HIGH_DAYS,
                                                  atr_lookback_days=ATR_LOOKBACK_DAYS,
                                                  atr_stop_multiplier=ATR_STOP_MULTIPLIER,
                                                  atr_target_multiplier=ATR_TARGET_MULTIPLIER,
                                                  expected_hold_days=VOL_BREAK_EXPECTED_HOLD_DAYS):
    sql_close = text("SELECT symbol, close FROM intraday_bhavcopy WHERE trade_date = :d")
    df_close = pd.read_sql(sql_close, engine_obj, params={"d": target_date})
    if df_close.empty:
        logger.info("No close prices for %s", target_date)
        return pd.DataFrame()

    sql_prior_high = text(f"""
       SELECT symbol, MAX(high) AS prior_high
       FROM intraday_bhavcopy
       WHERE trade_date BETWEEN DATE_SUB(:d, INTERVAL :lookback DAY) AND DATE_SUB(:d, INTERVAL 1 DAY)
       GROUP BY symbol
    """)
    df_high = pd.read_sql(sql_prior_high, engine_obj, params={"d": target_date, "lookback": lookback_high_days})

    atr_feature_name = f"atr_{atr_lookback_days}"
    sql_atr = text("SELECT symbol, value AS atr FROM strategy_features WHERE trade_date = :d AND feature_name = :feat")
    df_atr = pd.read_sql(sql_atr, engine_obj, params={"d": target_date, "feat": atr_feature_name})

    merged = df_close.merge(df_high, on="symbol", how="inner").merge(df_atr, on="symbol", how="inner")
    df_breakouts = merged[merged["close"] > merged["prior_high"]].copy()

    rows = []
    params = {
        "lookback_high_days": lookback_high_days,
        "atr_lookback_days": atr_lookback_days,
        "atr_stop_mult": atr_stop_multiplier,
        "atr_target_mult": atr_target_multiplier
    }

    for _, r in df_breakouts.iterrows():
        # skip if ATR missing or invalid
        if pd.isna(r.get("atr")):
            continue

        symbol = r["symbol"]
        try:
            close_price = float(r["close"])
        except Exception:
            continue
        try:
            prior_high = float(r["prior_high"]) if r["prior_high"] is not None else 0.0
        except Exception:
            prior_high = 0.0
        try:
            atr_val = float(r["atr"]) if r.get("atr") is not None else None
        except Exception:
            atr_val = None

        stop_price = close_price - atr_stop_multiplier * (atr_val if atr_val is not None else 0.0)
        target_price = close_price + atr_target_multiplier * (atr_val if atr_val is not None else 0.0)
        score = float((close_price - prior_high) / prior_high) if prior_high != 0 else 0.0

        # --- compute expected hold days using helper (safe call) ---
        try:
            est_days, _ = estimate_expected_hold_days(
                engine=engine_obj,
                symbol=symbol,
                strategy="volatility_breakout",
                score=score,
                atr_value=atr_val,
                lookback_days=252,
                pct_stop=0.005,
                pct_target=0.01,
                max_search_days=30,
                min_samples=8,
                atr_target_mult=atr_target_multiplier,
                avg_daily_move_factor=1.0,
                fallback_default=expected_hold_days,
                min_days=1,
                max_days=30,
                use_cache=True
            )
        except Exception:
            est_days = expected_hold_days

        # --- build row dict (no executable code inside dict) ---
        row = {
            "trade_date": target_date,
            "strategy": "volatility_breakout",
            "version": "v1",
            "params": json.dumps(params),
            "symbol": symbol,
            "signal_type": "LONG",
            "signal_score": score,
            "entry_model": "next_open",
            "qty": None,
            "entry_price": None,
            "stop_price": round(float(stop_price), 6) if stop_price is not None else None,
            "target_price": round(float(target_price), 6) if target_price is not None else None,
            "expected_hold_days": int(est_days),
            "notes": None
        }
        rows.append(row)

    logger.info("Generated %d volatility breakout signals for %s", len(rows), target_date)
    return pd.DataFrame(rows)


# -------------------------
# Backfill helpers
# -------------------------
def get_latest_signal_date(engine_obj):
    q = text("SELECT MAX(trade_date) AS last_date FROM strategy_signals")
    df = pd.read_sql(q, engine_obj)
    if df.empty or df["last_date"].isna().all():
        return None
    return pd.to_datetime(df["last_date"].iloc[0]).date().isoformat()


def get_latest_bhavcopy_date(engine_obj):
    q = text("SELECT MAX(trade_date) AS last_date FROM intraday_bhavcopy")
    df = pd.read_sql(q, engine_obj)
    if df.empty or df["last_date"].isna().all():
        return None
    return pd.to_datetime(df["last_date"].iloc[0]).date().isoformat()


def backfill_missing_signals(engine_obj, start_date_iso=None, end_date_iso=None, behavior_cfg=None):
    if start_date_iso and end_date_iso:
        all_dates_df = pd.read_sql(
            text("SELECT DISTINCT trade_date FROM intraday_bhavcopy WHERE trade_date BETWEEN :s AND :e ORDER BY trade_date"),
            engine_obj,
            params={"s": start_date_iso, "e": end_date_iso}
        )
        all_dates = all_dates_df["trade_date"].tolist()
    else:
        last_signal_date = get_latest_signal_date(engine_obj)
        last_bhavcopy_date = get_latest_bhavcopy_date(engine_obj)
        if last_bhavcopy_date is None:
            logger.error("No bhavcopy data found. Nothing to backfill.")
            return
        if last_signal_date is None:
            logger.info("No existing signals; backfilling all bhavcopy dates.")
            all_dates = pd.read_sql("SELECT DISTINCT trade_date FROM intraday_bhavcopy ORDER BY trade_date", engine_obj)["trade_date"].tolist()
        else:
           all_dates_df = pd.read_sql(
                text("SELECT DISTINCT trade_date FROM intraday_bhavcopy WHERE trade_date > :d ORDER BY trade_date"),
                engine_obj,
                params={"d": last_signal_date}
                    )
           all_dates = all_dates_df["trade_date"].tolist()

    if not all_dates:
        logger.info("No missing dates to process.")
        return

    all_dates_iso = [pd.to_datetime(d).date().isoformat() for d in all_dates]
    logger.info("Backfilling %d dates from %s to %s", len(all_dates_iso), all_dates_iso[0], all_dates_iso[-1])
    for date_iso in all_dates_iso:
        try:
            run_signal_generation_for_date(date_iso, engine_obj=engine_obj, behavior_cfg=behavior_cfg)
        except Exception:
            logger.exception("Failed while backfilling date %s", date_iso)


# -------------------------
# Pipeline orchestration
# -------------------------
def run_signal_generation_for_date(target_date_iso: str, engine_obj=None, behavior_cfg=None):
    if engine_obj is None:
        raise ValueError("engine_obj is required")
    if behavior_cfg is None:
        behavior_cfg = DEFAULT_CONFIG["behavior"]

    preview_only = behavior_cfg.get("preview_only", False)
    dry_run = behavior_cfg.get("dry_run", True)

    logger.info("Starting pipeline for %s (preview_only=%s, dry_run=%s)", target_date_iso, preview_only, dry_run)

    # 1) compute features
    df_features = compute_daily_features_for_date(engine_obj, target_date_iso)

    # If preview_only: do not upsert; end after generating data
    if preview_only:
        logger.info("Preview mode: skipping DB upserts. Computed features: %d", len(df_features))
        # NOTE: CSV writing intentionally omitted as per your request; kept in config for future use
        return

    # 2) upsert features (honoring dry_run)
    upsert_features(engine_obj, df_features, dry_run=dry_run)

    # 3) generate signals
    df_mom = generate_momentum_signals_for_date(engine_obj, target_date_iso)
    df_gap = generate_gap_follow_signals_for_date(engine_obj, target_date_iso)
    df_vol = generate_volatility_breakout_signals_for_date(engine_obj, target_date_iso)

    df_all_signals = pd.concat([df for df in (df_mom, df_gap, df_vol) if not df.empty], ignore_index=True, sort=False) if any([not df.empty for df in (df_mom, df_gap, df_vol)]) else pd.DataFrame()
    df_all_signals = enrich_signals_with_stops_targets(engine_obj, df_all_signals,
                                                   atr_lookback=14, atr_stop_mult=1.5, atr_target_mult=3.0)
    
    # ...existing code...
    df_all_signals = enrich_signals_with_stops_targets(engine_obj, df_all_signals,
                                                   atr_lookback=14, atr_stop_mult=1.5, atr_target_mult=3.0)

    # ----------------------------------------
    # Apply Liquidity Filtering + Scoring
    # ----------------------------------------
    # prefer liquidity rules from behavior_cfg if provided, else fall back to DEFAULT_CONFIG
    liquidity_rules = DEFAULT_CONFIG.get("liquidity_rules", {})
    if isinstance(behavior_cfg, dict) and "liquidity_rules" in behavior_cfg:
        liquidity_rules = behavior_cfg.get("liquidity_rules", liquidity_rules)

    if df_all_signals is None or df_all_signals.empty:
        logger.info("No signals generated for %s after enrichment; skipping liquidity filters and upsert.", target_date_iso)
    else:
        # ensure 'symbol' exists
        if "symbol" not in df_all_signals.columns:
            logger.warning("No 'symbol' column present in signals; skipping liquidity filtering.")
        else:
            unique_symbols = df_all_signals["symbol"].unique().tolist()

            # Step 1: compute raw liquidity metrics
            liquidity_df = compute_liquidity_metrics(
                engine_obj,
                unique_symbols,
                target_date_iso,
                lookback_days=20
            )

            # Step 2: apply scoring + hard rules
            df_all_signals = apply_liquidity_filters(
                df_all_signals,
                liquidity_df,
                liquidity_rules,
                mode=liquidity_rules.get("mode", "tag_only")
            )

            # Step 3: push liquidity info into params JSON (store as JSON-text)
            def _ensure_params_obj(p):
                if isinstance(p, dict):
                    return p
                if isinstance(p, str):
                    try:
                        return json.loads(p)
                    except Exception:
                        return {}
                return {}

            if "params" in df_all_signals.columns:
                df_all_signals["params"] = df_all_signals.apply(
                    lambda row: json.dumps({**_ensure_params_obj(row.get("params")), "liquidity": row.get("liquidity_info")}),
                    axis=1
                )
            else:
                df_all_signals["params"] = df_all_signals["liquidity_info"].apply(lambda x: json.dumps({"liquidity": x}))

    logger.info("Total signals generated for %s: %d", target_date_iso, 0 if df_all_signals is None else len(df_all_signals))

    # 4) upsert signals (honoring dry_run)
    upsert_signals(engine_obj, df_all_signals, dry_run=dry_run)

    # 5) record run
    summary = {
        "num_signals": int(len(df_all_signals)) if not df_all_signals.empty else 0,
        "counts_by_strategy": df_all_signals['strategy'].value_counts().to_dict() if not df_all_signals.empty else {}
    }
    run_name = f"signals_{target_date_iso}"
    run_params = {
        "momentum": {"lookback_days": MOMENTUM_LOOKBACK_DAYS, "top_n": MOMENTUM_TOP_N},
        "gap": {"threshold_pct": GAP_MINIMUM_PERCENT},
        "volatility_breakout": {"lookback_high_days": VOL_BREAK_LOOKBACK_HIGH_DAYS, "atr_lookback_days": ATR_LOOKBACK_DAYS}
    }
    record_run(engine_obj, run_name, run_params, run_summary=summary, dry_run=dry_run)
    logger.info("Finished pipeline for %s: %s", target_date_iso, summary)


# -------------------------
# CLI entrypoint
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Generate persisted signals (configured)")
    p.add_argument("--config", help="JSON config file that overrides DEFAULT_CONFIG", required=False)
    p.add_argument("--date", "-d", help="Target date (YYYY-MM-DD). Defaults to yesterday if omitted.", required=False)
    p.add_argument("--backfill-only", action="store_true", help="Only run backfill (no single-date job).")
    p.add_argument("--backfill-first", action="store_true", help="Run backfill first (default).")
    p.add_argument("--start-date", help="Optional explicit start date for backfill range (YYYY-MM-DD).", required=False)
    p.add_argument("--end-date", help="Optional explicit end date for backfill range (YYYY-MM-DD).", required=False)
    return p.parse_args()


if __name__ == "__main__":
    """
    Dual-mode entrypoint:

    - IDE / VS Code run (no command-line args):
        Uses DEFAULT_CONFIG and runs normal pipeline for yesterday (backfill-first behavior).
    - CLI run (with args):
        Uses parse_args() to control behavior (config file override, explicit date, backfill-only, ranges).
    """


    try:
        # If command-line args are present (other than the script name) use CLI mode
        if len(sys.argv) > 1:
            args = parse_args()  # uses argparse defined earlier
            # load config (file overrides DEFAULT_CONFIG)
            cfg = load_config(args.config) if getattr(args, "config", None) else DEFAULT_CONFIG
            engine = connect_db(cfg.get("db"))
            behavior_cfg = cfg.get("behavior", DEFAULT_CONFIG["behavior"])

            # Determine target date (explicit or default to yesterday)
            target_date = args.date if getattr(args, "date", None) else (date.today() - timedelta(days=1)).isoformat()

            # Backfill-only mode
            if getattr(args, "backfill_only", False):
                logger.info("CLI: running backfill-only from args")
                backfill_missing_signals(engine, start_date_iso=args.start_date, end_date_iso=args.end_date, behavior_cfg=behavior_cfg)
            else:
                # Optional: run backfill first if requested
                if getattr(args, "backfill_first", False):
                    logger.info("CLI: running backfill first as requested")
                    backfill_missing_signals(engine, start_date_iso=args.start_date, end_date_iso=args.end_date, behavior_cfg=behavior_cfg)

                # Run pipeline for the target date
                logger.info("CLI: running pipeline for date %s", target_date)
                run_signal_generation_for_date(target_date, engine_obj=engine, behavior_cfg=behavior_cfg)

        else:
            # IDE / VS Code run (no CLI args). Use defaults to make iteration fast & easy.
            logger.info("No CLI args detected — running in IDE mode using DEFAULT_CONFIG")

            cfg = DEFAULT_CONFIG
            engine = connect_db(cfg.get("db"))
            behavior_cfg = cfg.get("behavior", DEFAULT_CONFIG["behavior"])

            # Default behavior: backfill-first then run yesterday

            default_date = trading_day(engine)
            if behavior_cfg.get("preview_only", False):
                # preview_only: just compute features/signals, skip DB upserts
                logger.info("IDE mode: preview_only is True; computing features/signals without DB writes")
                run_signal_generation_for_date(default_date, engine_obj=engine, behavior_cfg=behavior_cfg)
            else:
                # Run backfill (if any missing dates) before the main date
                logger.info("IDE mode: running backfill-first (if needed) and then pipeline for %s", default_date)
                backfill_missing_signals(engine, behavior_cfg=behavior_cfg)
                last_signal_date = get_latest_signal_date(engine)
                if last_signal_date is None or pd.to_datetime(last_signal_date).date() < pd.to_datetime(default_date).date():
                    logger.info("Running pipeline for default_date %s (not covered by backfill)", default_date)
                    run_signal_generation_for_date(default_date, engine_obj=engine, behavior_cfg=behavior_cfg)
                else:
                    logger.info("Default date %s already covered by backfill (latest signal date: %s) — skipping", default_date, last_signal_date)
                
    except Exception:
        logger.exception("Top-level pipeline failure")
        raise
