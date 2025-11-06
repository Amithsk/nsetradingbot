#!/usr/bin/env python3
"""
NSEDBlabelintradayevaluation.py

Main orchestrator script. Uses:
  - eval_logic.decide_outcome  (pure labeling logic)
  - db_io.*                    (DB reads/writes)

Features:
  - incremental (DB-driven) processing of unprocessed trade dates
  - backfill: last-N trade dates or explicit start/end date range
  - idempotent upserts into signal_evaluation_results using eval_run_tag
  - logs summary counts

Usage examples:
  # incremental (process unprocessed trade dates discovered in intraday_bhavcopy)
  python NSEDBlabelintradayevaluation.py --tag intraday_v1

  # backfill last 30 trade dates (based on intraday_bhavcopy)
  python NSEDBlabelintradayevaluation.py --tag intraday_v1_backfill --last-n 30

  # full backfill for explicit date range
  python NSEDBlabelintradayevaluation.py --tag intraday_v1_backfill_range --start 2024-01-01 --end 2024-12-31 --force-full

  # incremental but reprocess last day as overlap (safety)
  python NSEDBlabelintradayevaluation.py --tag intraday_v1 --overlap 1
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

# --- Make sure project root is in sys.path so we can import from Code/*
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]  # go up from Code/db to project root (nsetradingbot)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import evaluation logic and DB IO modules (ensure they're on PYTHONPATH)
from Code.utils.nseintradaytradeevallogic import decide_outcome
from Code.utils.nseintradaytradeevallogicdb import fetch_signals_for_date,fetch_bhavcopy_on_dates,upsert_eval_rows,get_unprocessed_trade_dates 

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("NSEDBLabelIntradayEvaluation")

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

def _recent_trade_dates(engine, n: int) -> List[date]:
    """
    Return last n distinct trade_date values present in intraday_bhavcopy (descending -> ascending).
    """
    if n <= 0:
        return []
    sql = text("""
        SELECT DISTINCT trade_date
        FROM intraday_bhavcopy
        ORDER BY trade_date DESC
        LIMIT :n
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"n": int(n)}).fetchall()
    dates = [pd.to_datetime(r['trade_date']).date() for r in rows]
    return sorted(dates)


def _trade_dates_from_range(engine, start_dt: date, end_dt: date) -> List[date]:
    """
    Return trade dates in intraday_bhavcopy between start_dt and end_dt inclusive.
    """
    sql = text("""
        SELECT DISTINCT trade_date
        FROM intraday_bhavcopy
        WHERE trade_date BETWEEN :s AND :e
        ORDER BY trade_date
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"s": start_dt.isoformat(), "e": end_dt.isoformat()}).fetchall()
    return [pd.to_datetime(r['trade_date']).date() for r in rows]


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

    df_signals['signal_date'] = pd.to_datetime(df_signals['signal_date']).dt.date

    # fetch corresponding bhavcopy rows
    logger.info("Fetching bhavcopy for %d symbols on %s", len(df_signals['symbol'].unique()), trade_date)
    df_bhav = fetch_bhavcopy_on_dates(engine, df_signals[['symbol', 'signal_date']])
    if df_bhav.empty:
        # No bhav rows — return DataFrame marking missing bhavcopy entries
        logger.warning("No bhavcopy rows found for date %s", trade_date)
        rows = []
        for _, r in df_signals.iterrows():
            rows.append({
                'signal_id': int(r['signal_id']),
                'symbol': r['symbol'],
                'signal_date': r['signal_date'].isoformat(),
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

    # Merge
    df_merged = df_signals.merge(df_bhav, left_on=['symbol', 'signal_date'], right_on=['symbol', 'trade_date'], how='left')

    rows = []
    # Use defaults if provided, else leave None so eval_logic uses its defaults
    for _, r in df_merged.iterrows():
        if pd.isna(r['open']):
            rows.append({
                'signal_id': int(r['signal_id']),
                'symbol': r['symbol'],
                'signal_date': r['signal_date'].isoformat(),
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
            'signal_date': r['signal_date'].isoformat(),
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


def _process_dates_and_persist(engine, trade_dates: List[date], eval_run_tag: str, defaults: Optional[dict] = None):
    """
    For each trade_date: evaluate and persist via db_io.upsert_eval_rows.
    Returns summary dict.
    """
    total = 0
    wins = 0
    losses = 0
    neutral = 0
    ambiguous = 0
    processed_dates = []
    for dt in trade_dates:
        logger.info("Evaluating %s", dt)
        try:
            df_eval = evaluate_signals_for_date(engine, dt, eval_run_tag, defaults)
            if df_eval is None or df_eval.empty:
                logger.info("No eval rows for %s", dt)
            else:
                # Persist
                upsert_eval_rows(engine, df_eval)
                total += len(df_eval)
                wins += (df_eval['label_outcome'] == 'win').sum()
                losses += (df_eval['label_outcome'] == 'loss').sum()
                neutral += (df_eval['label_outcome'] == 'neutral').sum()
                ambiguous += df_eval['ambiguous_flag'].sum()
                processed_dates.append(dt)
                logger.info("Persisted %d eval rows for %s", len(df_eval), dt)
        except Exception as ex:
            logger.exception("Error processing %s: %s", dt, ex)

    summary = {
        'processed_dates': processed_dates,
        'total_rows': total,
        'wins': int(wins),
        'losses': int(losses),
        'neutral': int(neutral),
        'ambiguous': int(ambiguous)
    }
    return summary


def main():
    parser = argparse.ArgumentParser(description="NSE DB Label Intraday Evaluation - orchestrator")
    parser.add_argument("--db-uri", required=False, help="SQLAlchemy DB URI (if omitted uses environment default)", default=None)
    parser.add_argument("--tag", required=True, help="eval_run_tag to use for this run (e.g. intraday_v1)")
    parser.add_argument("--force-full", action="store_true", help="Force full backfill using --start/--end (must provide start & end)")
    parser.add_argument("--start", help="Start date for full backfill (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date for full backfill (YYYY-MM-DD)")
    parser.add_argument("--last-n", type=int, default=0, help="Backfill last N trade dates (based on intraday_bhavcopy). If >0, uses last-N instead of incremental.")
    parser.add_argument("--overlap", type=int, default=0, help="Include previous N trade dates as overlap (safety) for incremental runs.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # create engine: prefer passed DB URI, else rely on your app's config mechanism
    if args.db_uri:
        
        engine = connect_db(args.db_uri)
    else:
        # Use default environment/config from your project; replace below with your loader if available
        # e.g., from config import DEFAULT_DB_URI
        DEFAULT_DB_URI = "mysql+pymysql://user:pass@host:3306/dbname"
        engine = connect_db(DEFAULT_DB_URI)
        logger.info("No --db-uri provided; using default from script (replace with your config loader).")

    eval_run_tag = args.tag

    # Decide trade dates to process
    trade_dates: List[date] = []
    if args.force_full:
        if not args.start or not args.end:
            logger.error("--force-full requires --start and --end")
            return
        start_dt = pd.to_datetime(args.start).date()
        end_dt = pd.to_datetime(args.end).date()
        # Restrict to trade dates available in intraday_bhavcopy
        trade_dates = _trade_dates_from_range(engine, start_dt, end_dt)
        logger.info("Full backfill mode: %d trade dates found between %s and %s", len(trade_dates), start_dt, end_dt)
    elif args.last_n and args.last_n > 0:
        trade_dates = _recent_trade_dates(engine, args.last_n)
        logger.info("Last-N backfill mode: %d trade dates selected (last %d)", len(trade_dates), args.last_n)
    else:
        # incremental DB-driven approach: process unprocessed trade dates (based on eval_run_tag)
        trade_dates = get_unprocessed_trade_dates(engine, eval_run_tag)
        logger.info("Incremental mode: %d unprocessed trade dates found", len(trade_dates))
        if args.overlap and args.overlap > 0:
            # include previous N trade dates before earliest unprocessed date
            if trade_dates:
                earliest = trade_dates[0]
                sql_prev = text("""
                    SELECT DISTINCT trade_date FROM intraday_bhavcopy
                    WHERE trade_date < :d
                    ORDER BY trade_date DESC LIMIT :n
                """)
                with engine.connect() as conn:
                    prev_rows = conn.execute(sql_prev, {"d": earliest.isoformat(), "n": int(args.overlap)}).fetchall()
                prev_dates = [pd.to_datetime(r['trade_date']).date() for r in prev_rows]
                trade_dates = sorted(set(prev_dates + trade_dates))
                logger.info("Applied overlap: now %d dates to process", len(trade_dates))
            else:
                logger.info("No unprocessed dates; overlap ignored.")

    if not trade_dates:
        logger.info("No trade dates to process; exiting.")
        return

    # Optionally prepare defaults for decide_outcome - leave None to use eval_logic defaults
    defaults = None

    start_time = datetime.utcnow()
    logger.info("Starting evaluation run tag=%s at %s, processing %d dates", eval_run_tag, start_time.isoformat(), len(trade_dates))
    summary = _process_dates_and_persist(engine, trade_dates, eval_run_tag, defaults)
    end_time = datetime.utcnow()
    logger.info("Finished evaluation run at %s", end_time.isoformat())
    logger.info("Summary: processed_dates=%s total_rows=%d wins=%d losses=%d neutral=%d ambiguous=%d",
                summary['processed_dates'], summary['total_rows'], summary['wins'], summary['losses'],
                summary['neutral'], summary['ambiguous'])


if __name__ == "__main__":
    main()
