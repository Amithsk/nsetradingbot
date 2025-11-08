#!/usr/bin/env python3
"""
nseintradaylabelevaluationdbupdate.py

Orchestrator 
 - Uses detect_intraday_columns() so intraday_bhavcopy column-name variants are handled.
 - Uses expanding bindparams for IN-lists where needed.
 - Uses logging (no prints).
"""
import argparse
import os
import urllib.parse
import logging
from datetime import date, datetime
from typing import List, Optional
from sqlalchemy import create_engine, text
import pandas as pd
import sys
from pathlib import Path
from functools import lru_cache

# --- Make sure project root is in sys.path so we can import from Code/*
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]  # adjust if your folder depth differs
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import evaluation logic and DB IO modules
from Code.utils.nseintradaytradeevallogic import decide_outcome
from Code.utils.nseintradaytradeevallogicdb import (
    fetch_signals_for_date,
    fetch_bhavcopy_on_dates,
    upsert_eval_rows,
    upsert_eval_rows_conn,
    get_unprocessed_trade_dates,
)
from Code.utils.nseintraday_db_utils import detect_intraday_columns

# ----------------- CONFIG you can edit in VS Code -----------------
TRANSACTION_PREVIEW_DEFAULT = True  # If True, all DB writes are ROLLED BACK at the end (preview mode)
DEFAULT_EVAL_RUN_TAG = "intraday_v1_default"

# DEFAULT_CONFIG used for IDE (no-args) mode — edit for safe IDE testing
DEFAULT_CONFIG = {
    "db": {
        "host": "localhost",
        "user": "root",
        "password": "your_password",
        "db": "intradaytrading",
        "port": 3306
    },
    "eval_run_tag": DEFAULT_EVAL_RUN_TAG,
    "transaction_preview": TRANSACTION_PREVIEW_DEFAULT,
    # optional: last_n for a quick IDE backfill; set to 0 to use incremental unprocessed dates
    "last_n": 3,
    # optional: overlap days for safety in IDE mode
    "overlap": 0
}
# -----------------------------------------------------------------

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("NSEDBLabelIntradayEvaluation")


def connect_db(db_cfg: Optional[object] = None):
    """
    Create and return a SQLAlchemy engine.
    Accepts:
      - db_cfg as dict with keys user/password/host/port/db OR
      - db_cfg as full SQLAlchemy URI string
      - None -> use environment variables
    """
    if isinstance(db_cfg, str) and (db_cfg.startswith("mysql://") or db_cfg.startswith("mysql+pymysql://") or db_cfg.startswith("postgresql://")):
        DATABASE_URL = db_cfg
        engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600, echo=False)
        logger.info("SQLAlchemy engine created from provided URI.")
        return engine

    env_password = os.getenv('MYSQL_PASSWORD')
    if db_cfg and isinstance(db_cfg, dict):
        user = db_cfg.get("user", "root")
        password = db_cfg.get("password", "") if env_password is None else env_password
        host = db_cfg.get("host", "localhost")
        port = db_cfg.get("port", 3306)
        dbname = db_cfg.get("db", "intradaytrading")
    else:
        user = os.getenv("MYSQL_USER", "root")
        password = env_password if env_password is not None else os.getenv("MYSQL_PASSWORD", "")
        host = os.getenv("MYSQL_HOST", "localhost")
        port = int(os.getenv("MYSQL_PORT", "3306"))
        dbname = os.getenv("MYSQL_DB", "intradaytrading")

    encoded_pw = urllib.parse.quote_plus(password if password is not None else "")
    DATABASE_URL = f"mysql+pymysql://{user}:{encoded_pw}@{host}:{port}/{dbname}"
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600, echo=False)
    logger.info("SQLAlchemy engine created for DB: %s", dbname)
    return engine


# ---------------------- detection cache helpers ----------------------
@lru_cache(maxsize=8)
def _cached_detect_intraday_columns(db_url: str) -> dict:
    """
    Cache detect_intraday_columns results per DB URL string.
    We create a temp engine for detection and dispose it.
    """
    # create a temporary engine for detection
    tmp_eng = create_engine(db_url, pool_pre_ping=True, pool_recycle=3600, echo=False)
    try:
        colmap = detect_intraday_columns(tmp_eng)
    finally:
        try:
            tmp_eng.dispose()
        except Exception:
            pass
    return colmap


def _get_colmap_for_engine(engine):
    """
    Return cached detect_intraday_columns mapping for the given engine.
    """
    try:
        db_url = engine.url.render_as_string(hide_password=True)
    except Exception:
        db_url = repr(engine)
    return _cached_detect_intraday_columns(db_url)


# ---------------------- DB helper functions ----------------------
def _recent_trade_dates(engine, n: int) -> List[date]:
    """
    Return last n distinct trade_date values present in intraday_bhavcopy (descending -> ascending).
    Uses detected date column name.
    """
    if n <= 0:
        return []
    colmap = _get_colmap_for_engine(engine)
    date_col = colmap.get('trade_date')
    if not date_col:
        raise RuntimeError("Could not detect trade_date column in intraday_bhavcopy (for _recent_trade_dates).")

    # Use param binding for LIMIT :n (works with SQLAlchemy)
    sql = text(f"""
        SELECT DISTINCT {date_col} AS trade_date
        FROM intraday_bhavcopy
        ORDER BY {date_col} DESC
        LIMIT :n
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"n": int(n)})
        try:
            rows = result.mappings().all()
            dates = [pd.to_datetime(r['trade_date']).date() for r in rows]
        except Exception:
            rows = result.fetchall()
            dates = [pd.to_datetime(r[0]).date() for r in rows]
    return sorted(dates)


def _trade_dates_from_range(engine, start_dt: date, end_dt: date) -> List[date]:
    """
    Return trade dates in intraday_bhavcopy between start_dt and end_dt inclusive.
    Uses detected date column name.
    """
    colmap = _get_colmap_for_engine(engine)
    date_col = colmap.get('trade_date')
    if not date_col:
        raise RuntimeError("Could not detect trade_date column in intraday_bhavcopy (for range query).")

    sql = text(f"""
        SELECT DISTINCT {date_col} AS trade_date
        FROM intraday_bhavcopy
        WHERE {date_col} BETWEEN :s AND :e
        ORDER BY {date_col}
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"s": start_dt, "e": end_dt})
        try:
            rows = result.mappings().all()
            dates = [pd.to_datetime(r['trade_date']).date() for r in rows]
        except Exception:
            rows = result.fetchall()
            dates = [pd.to_datetime(r[0]).date() for r in rows]
    return dates


# ---------------------- evaluation logic ----------------------
def evaluate_signals_for_date(engine, trade_date: date, eval_run_tag: str,
                              defaults: Optional[dict] = None) -> pd.DataFrame:
    """
    Evaluate all entry_model='open' signals for a trade_date and return DataFrame ready for upsert.
    This function performs NO DB writes; only reads via db_io and uses eval_logic.decide_outcome.
    """
    logger.info("Fetching signals for %s", trade_date)
    df_signals = fetch_signals_for_date(engine, trade_date)
    if df_signals.empty:
        logger.info("No signals found for %s", trade_date)
        return pd.DataFrame()

    df_signals['trade_date'] = pd.to_datetime(df_signals['trade_date']).dt.date

    logger.info("Fetching bhavcopy for %d symbols on %s", len(df_signals['symbol'].unique()), trade_date)
    df_bhav = fetch_bhavcopy_on_dates(engine, df_signals[['symbol', 'trade_date']])
    if df_bhav.empty:
        logger.warning("No bhavcopy rows found for date %s", trade_date)
        rows = []
        for _, r in df_signals.iterrows():
            rows.append({
                'signal_id': int(r['signal_id']),
                'symbol': r['symbol'],
                'trade_date': r['trade_date'].isoformat(),
                'strategy': r.get('strategy'),
                'entry_model': r.get('entry_model'),
                'entry_price': r.get('entry_price'),
                'stop_price': r.get('stop_price'),
                'target_price': r.get('target_price'),
                'entry_time': 'open',
                'realized_high': None, 'realized_low': None, 'close_price': None,
                'realized_return': None, 'exit_price': None, 'exit_reason': None,
                'days_to_exit': 0, 'label_outcome': 'neutral', 'ambiguous_flag': 0,
                'notes': 'missing bhavcopy for date', 'eval_run_tag': eval_run_tag
            })
        return pd.DataFrame(rows)

    df_merged = df_signals.merge(df_bhav, left_on=['symbol', 'trade_date'], right_on=['symbol', 'trade_date'], how='left')

    rows = []
    for _, r in df_merged.iterrows():
        # missing bhav row
        if pd.isna(r.get('open')):
            rows.append({
                'signal_id': int(r['signal_id']),
                'symbol': r['symbol'],
                'trade_date': r['trade_date'].isoformat(),
                'strategy': r.get('strategy'),
                'entry_model': r.get('entry_model'),
                'entry_price': r.get('entry_price'),
                'stop_price': r.get('stop_price'),
                'target_price': r.get('target_price'),
                'entry_time': 'open',
                'realized_high': None, 'realized_low': None, 'close_price': None,
                'realized_return': None, 'exit_price': None, 'exit_reason': None,
                'days_to_exit': 0, 'label_outcome': 'neutral', 'ambiguous_flag': 0,
                'notes': 'missing bhavcopy row', 'eval_run_tag': eval_run_tag
            })
            continue

        signal_row = r.to_dict()
        bhav_row = r.to_dict()
        outcome = decide_outcome(signal_row, bhav_row, defaults if defaults else None)

        rows.append({
            'signal_id': int(r['signal_id']),
            'symbol': r['symbol'],
            'trade_date': r['trade_date'].isoformat(),
            'strategy': r.get('strategy'),
            'entry_model': r.get('entry_model'),
            'entry_price': outcome['entry_price_used'],
            'stop_price': outcome['stop_price'],
            'target_price': outcome['target_price'],
            'entry_time': 'open',
            'realized_high': outcome['realized_high'],
            'realized_low': outcome['realized_low'],
            'close_price': outcome['close_price'],
            'realized_return': outcome['realized_return'],
            'exit_price': outcome['exit_price'],
            'exit_reason': outcome['exit_reason'],
            'days_to_exit': 0,
            'label_outcome': outcome['label_outcome'],
            'ambiguous_flag': outcome['ambiguous_flag'],
            'notes': outcome['notes'],
            'eval_run_tag': eval_run_tag
        })

    df_out = pd.DataFrame(rows)
    return df_out


def _process_dates_and_persist(engine, trade_dates: List[date], eval_run_tag: str, defaults: Optional[dict] = None,
                               transaction_preview: bool = False):
    """
    For each trade_date: evaluate and persist via db_io.upsert_eval_rows.
    If transaction_preview=True, uses upsert_eval_rows_conn inside a single transaction and ROLLBACKs at the end.
    Returns summary dict.
    """
    total = wins = losses = neutral = ambiguous = 0
    processed_dates: List[date] = []

    if transaction_preview:
        logger.warning("Running in TRANSACTION PREVIEW mode — all writes will be rolled back at the end.")
        conn = engine.connect()
        trans = conn.begin()
        try:
            for dt in trade_dates:
                logger.info("Evaluating %s (preview)", dt)
                try:
                    df_eval = evaluate_signals_for_date(engine, dt, eval_run_tag, defaults)
                    if df_eval is None or df_eval.empty:
                        logger.info("No eval rows for %s", dt)
                        continue
                    upsert_eval_rows_conn(conn, df_eval)
                    total += len(df_eval)
                    wins += int((df_eval['label_outcome'] == 'win').sum()) if 'label_outcome' in df_eval else 0
                    losses += int((df_eval['label_outcome'] == 'loss').sum()) if 'label_outcome' in df_eval else 0
                    neutral += int((df_eval['label_outcome'] == 'neutral').sum()) if 'label_outcome' in df_eval else 0
                    ambiguous += int(df_eval['ambiguous_flag'].sum()) if 'ambiguous_flag' in df_eval else 0
                    processed_dates.append(dt)
                    logger.info("WROTE %d rows for %s (in tx preview)", len(df_eval), dt)
                except Exception as ex:
                    logger.exception("Error processing %s in preview mode: %s", dt, ex)
                    trans.rollback()
                    raise
            trans.rollback()
            logger.info("Transaction preview run: all changes rolled back successfully.")
        finally:
            conn.close()
    else:
        for dt in trade_dates:
            logger.info("Evaluating %s", dt)
            try:
                df_eval = evaluate_signals_for_date(engine, dt, eval_run_tag, defaults)
                if df_eval is None or df_eval.empty:
                    logger.info("No eval rows for %s", dt)
                else:
                    upsert_eval_rows(engine, df_eval)
                    total += len(df_eval)
                    wins += int((df_eval['label_outcome'] == 'win').sum()) if 'label_outcome' in df_eval else 0
                    losses += int((df_eval['label_outcome'] == 'loss').sum()) if 'label_outcome' in df_eval else 0
                    neutral += int((df_eval['label_outcome'] == 'neutral').sum()) if 'label_outcome' in df_eval else 0
                    ambiguous += int(df_eval['ambiguous_flag'].sum()) if 'ambiguous_flag' in df_eval else 0
                    processed_dates.append(dt)
                    logger.info("Persisted %d eval rows for %s", len(df_eval), dt)
            except Exception as ex:
                logger.exception("Error processing %s: %s", dt, ex)

    return {
        'processed_dates': processed_dates,
        'total_rows': total,
        'wins': int(wins),
        'losses': int(losses),
        'neutral': int(neutral),
        'ambiguous': int(ambiguous)
    }


# ---------------------------
# Entry / run-mode selection
# ---------------------------
def parse_args_wrapper():
    p = argparse.ArgumentParser(description="NSE DB Label Intraday Evaluation - orchestrator")
    p.add_argument("--db-uri", required=False, help="SQLAlchemy DB URI (if omitted uses environment/default)", default=None)
    p.add_argument("--tag", required=False, help="eval_run_tag to use for this run (e.g. intraday_v1). If omitted, DEFAULT_EVAL_RUN_TAG is used.")
    p.add_argument("--force-full", action="store_true", help="Force full backfill using --start/--end")
    p.add_argument("--start", help="Start date for full backfill (YYYY-MM-DD)")
    p.add_argument("--end", help="End date for full backfill (YYYY-MM-DD)")
    p.add_argument("--last-n", type=int, default=0, help="Backfill last N trade dates (based on intraday_bhavcopy).")
    p.add_argument("--overlap", type=int, default=0, help="Include previous N trade dates as overlap (safety).")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--rollback", action="store_true", help="Run in transaction-preview mode: write into DB but ROLLBACK at the end (preview).")
    return p.parse_args()


def _fetch_previous_n_dates_before(engine, before_date: date, n: int) -> List[date]:
    """
    Helper that returns previous N trade dates strictly before 'before_date'.
    Uses detected date column.
    """
    if n <= 0:
        return []
    colmap = _get_colmap_for_engine(engine)
    date_col = colmap.get('trade_date')
    if not date_col:
        raise RuntimeError("Could not detect trade_date column in intraday_bhavcopy (for prev dates).")

    sql = text(f"""
        SELECT DISTINCT {date_col} AS trade_date
        FROM intraday_bhavcopy
        WHERE {date_col} < :d
        ORDER BY {date_col} DESC
        LIMIT :n
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"d": before_date, "n": int(n)})
        try:
            rows = result.mappings().all()
            prev = [pd.to_datetime(r['trade_date']).date() for r in rows]
        except Exception:
            rows = result.fetchall()
            prev = [pd.to_datetime(r[0]).date() for r in rows]
    return prev


def main():
    import sys
    # CLI mode if args present, else IDE mode uses DEFAULT_CONFIG
    if len(sys.argv) > 1:
        args = parse_args_wrapper()
        if args.db_uri:
            try:
                engine = create_engine(args.db_uri, pool_pre_ping=True, pool_recycle=3600, echo=False)
                logger.info("SQLAlchemy engine created from --db-uri parameter.")
            except Exception:
                engine = connect_db(args.db_uri)
        else:
            engine = connect_db(None)

        eval_run_tag = args.tag if args.tag else (os.getenv("EVAL_RUN_TAG") or DEFAULT_EVAL_RUN_TAG)
        if args.debug:
            logger.setLevel(logging.DEBUG)
        transaction_preview_flag = args.rollback or TRANSACTION_PREVIEW_DEFAULT

        trade_dates: List[date] = []
        if args.force_full:
            if not args.start or not args.end:
                logger.error("--force-full requires --start and --end")
                return
            start_dt = pd.to_datetime(args.start).date()
            end_dt = pd.to_datetime(args.end).date()
            trade_dates = _trade_dates_from_range(engine, start_dt, end_dt)
            logger.info("Full backfill mode: %d trade dates found between %s and %s", len(trade_dates), start_dt, end_dt)
        elif args.last_n and args.last_n > 0:
            trade_dates = _recent_trade_dates(engine, args.last_n)
            logger.info("Last-N backfill mode: %d trade dates selected (last %d)", len(trade_dates), args.last_n)
        else:
            trade_dates = get_unprocessed_trade_dates(engine, eval_run_tag)
            logger.info("Incremental mode: %d unprocessed trade dates found", len(trade_dates))
            if args.overlap and args.overlap > 0 and trade_dates:
                earliest = trade_dates[0]
                prev_dates = _fetch_previous_n_dates_before(engine, earliest, args.overlap)
                trade_dates = sorted(set(prev_dates + trade_dates))
                logger.info("Applied overlap: now %d dates to process", len(trade_dates))

        if not trade_dates:
            logger.info("No trade dates to process; exiting.")
            return

        logger.info("CLI run: tag=%s transaction_preview=%s dates=%s", eval_run_tag, transaction_preview_flag, trade_dates)
        summary = _process_dates_and_persist(engine, trade_dates, eval_run_tag, defaults=None, transaction_preview=transaction_preview_flag)
        logger.info("Summary: %s", summary)

    else:
        # IDE mode: use DEFAULT_CONFIG
        logger.info("No CLI args detected — running in IDE mode using DEFAULT_CONFIG")
        engine = connect_db(DEFAULT_CONFIG.get("db"))
        eval_run_tag = os.getenv("EVAL_RUN_TAG") or DEFAULT_CONFIG.get("eval_run_tag") or DEFAULT_EVAL_RUN_TAG
        transaction_preview_flag = DEFAULT_CONFIG.get("transaction_preview", TRANSACTION_PREVIEW_DEFAULT)

        last_n = int(DEFAULT_CONFIG.get("last_n", 0) or 0)
        overlap = int(DEFAULT_CONFIG.get("overlap", 0) or 0)

        if last_n and last_n > 0:
            trade_dates = _recent_trade_dates(engine, last_n)
            logger.info("IDE last-n mode: selected %d recent trade dates", len(trade_dates))
        else:
            trade_dates = get_unprocessed_trade_dates(engine, eval_run_tag)
            logger.info("IDE incremental mode: %d unprocessed trade dates found", len(trade_dates))
            if overlap and overlap > 0 and trade_dates:
                earliest = trade_dates[0]
                prev_dates = _fetch_previous_n_dates_before(engine, earliest, overlap)
                trade_dates = sorted(set(prev_dates + trade_dates))
                logger.info("IDE applied overlap: now %d dates to process", len(trade_dates))

        if not trade_dates:
            logger.info("No trade dates to process in IDE mode; exiting.")
            return

        logger.info("IDE run: tag=%s transaction_preview=%s dates=%s", eval_run_tag, transaction_preview_flag, trade_dates)
        summary = _process_dates_and_persist(engine, trade_dates, eval_run_tag, defaults=None, transaction_preview=transaction_preview_flag)
        logger.info("IDE Summary: %s", summary)


if __name__ == "__main__":
    main()
