"""
nseintradaytradeevallogicdb.py

DB helper functions used by the label evaluation orchestrator.
Uses engine-object cache for column map detection (no create_engine in helper).
"""
import sqlalchemy as sa
import pandas as pd
from typing import List
from datetime import date
import logging
from pathlib import Path
import sys
import math
import numpy as np

# Ensure project root on path so relative imports work
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from Code.utils.nseintraday_db_utils import detect_intraday_columns

logger = logging.getLogger("nseintradaytradeevallogicdb")

# engine-object based cache
_colmap_cache_by_engine = {}

logger = logging.getLogger("nseintradaytradeevallogicdb")

def _is_writable_column(col: sa.Column) -> bool:
    """
    Conservative test whether it's reasonable to include this column in ON DUPLICATE KEY UPDATE.
    Exclude primary key, autoincrement, and server_default columns (like created_at).
    """
    try:
        if getattr(col, "primary_key", False):
            return False
        if getattr(col, "autoincrement", False):
            return False
        if getattr(col, "server_default", None) is not None:
            return False
        if getattr(col, "default", None) is not None:
            # conservative: don't auto-update columns with defaults
            return False
    except Exception:
        return False
    return True
def _sanitize_value(v):
    """
    Convert problematic scalar values to safe Python types:
      - pandas/Numpy NaN, NaT -> None
      - numpy scalar -> python scalar (item())
      - +/-inf -> None
      - pandas Timestamp/date -> ISO string (or python date if you prefer)
    """
    # pandas missing
    try:
        if v is None:
            return None
        # pandas NA (pd.NA)
        if v is pd.NA:
            return None
    except Exception:
        pass

    # numpy scalar types (np.float64, np.int64, np.bool_, etc.)
    if isinstance(v, np.generic):
        try:
            v = v.item()
        except Exception:
            # fallback to str
            v = float(v) if isinstance(v, (np.floating,)) else v

    # pandas Timestamp / datetime-like
    if isinstance(v, (pd.Timestamp, pd.DatetimeTZDtype)) or hasattr(v, "to_pydatetime"):
        try:
            # return ISO date/time string or datetime object - MySQL driver handles Python datetime
            return pd.to_datetime(v).to_pydatetime()
        except Exception:
            pass

    # float NaN or inf
    if isinstance(v, float):
        if math.isnan(v) or not math.isfinite(v):
            return None
        # normal float -> keep
        return v

    # numeric NaN checks for other types
    try:
        if isinstance(v, (int, str, bool)):
            return v
        # catch objects that represent numeric NaN via pandas
        if pd.isna(v):
            return None
    except Exception:
        pass

    # fallback: return value as-is
    return 

def _sanitize_records(records: list) -> list:
    """
    Walk list-of-dicts (records) and sanitize each value via _sanitize_value.
    Returns new list-of-dicts suitable for SQL binding.
    """
    out = []
    for rec in records:
        new_rec = {}
        for k, v in rec.items():
            new_rec[k] = _sanitize_value(v)
        out.append(new_rec)
    return out


def _get_colmap_for_engine(engine: sa.engine.Engine) -> dict:
    key = f"eng_{id(engine)}"
    if key in _colmap_cache_by_engine:
        return _colmap_cache_by_engine[key]
    colmap = detect_intraday_columns(engine)
    _colmap_cache_by_engine[key] = colmap
    return colmap


def fetch_signals_for_date(engine: sa.engine.Engine, trade_date: date) -> pd.DataFrame:
    """
    Fetch strategy_signals for a single trade_date where entry_model='open'.
    Returns a pandas DataFrame.
    """
    sql = sa.text("""
        SELECT id as signal_id, symbol, trade_date, strategy, entry_model,
               entry_price, stop_price, target_price
        FROM strategy_signals
        WHERE entry_model = 'open' AND trade_date = :d
    """)
    return pd.read_sql(sql, engine, params={"d": trade_date.isoformat()})


def fetch_bhavcopy_on_dates(engine: sa.engine.Engine, symbols_dates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch intraday_bhavcopy OHLC rows for the given (symbol, trade_date) pairs.
    Normalizes output columns to: symbol, trade_date, open, high, low, close
    """
    pairs = symbols_dates_df[['symbol', 'trade_date']].drop_duplicates()
    if pairs.empty:
        return pd.DataFrame()

    colmap = _get_colmap_for_engine(engine)

    # required mapped columns
    sym_col = colmap['symbol']
    date_col = colmap['trade_date']
    open_col = colmap['open']
    high_col = colmap['high']
    low_col = colmap['low']
    close_col = colmap['close']

    dates = sorted(pairs['trade_date'].unique())
    symbols = pairs['symbol'].unique().tolist()

    # Use expanding binds by passing tuples; SQLAlchemy will expand correctly
    sql = sa.text(f"""
        SELECT {sym_col} AS symbol,
               {date_col} AS trade_date,
               {open_col}  AS open,
               {high_col}  AS high,
               {low_col}   AS low,
               {close_col} AS close
        FROM intraday_bhavcopy
        WHERE {date_col} IN :dates AND {sym_col} IN :symbols
    """)

    df = pd.read_sql(sql, engine, params={"dates": tuple(dates), "symbols": tuple(symbols)})
    if not df.empty:
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    return df


def upsert_eval_rows(engine: sa.engine.Engine, df_rows: pd.DataFrame):
    """
    Upsert evaluation result rows into signal_evaluation_results using ON DUPLICATE KEY UPDATE.
    Sanitizes NaN/NaT/inf values before executing.
    """
    if df_rows is None or df_rows.empty:
        logger.info("upsert_eval_rows: nothing to upsert.")
        return

    metadata = sa.MetaData()
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=engine)

    # convert df -> records and sanitize
    records = df_rows.to_dict(orient='records')
    if not records:
        logger.info("upsert_eval_rows: no records after conversion.")
        return

    records = _sanitize_records(records)

    # determine insert payload columns from first record
    insert_columns = set(records[0].keys())

    insert_stmt = sa.dialects.mysql.insert(tbl).values(records)

    # pick table columns that are writable and present in payload
    writable_tbl_cols = [c for c in tbl.columns if _is_writable_column(c) and c.name in insert_columns]
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in writable_tbl_cols}

    if not update_cols:
        # nothing safe to update - do plain insert (duplicates will raise) but we log this
        logger.warning("upsert_eval_rows: no writable columns intersecting insert payload. Performing plain INSERT (duplicates will error).")
        try:
            with engine.begin() as conn:
                conn.execute(insert_stmt)
            logger.info("Inserted %d rows (no update).", len(records))
        except Exception as exc:
            logger.exception("Plain INSERT failed during upsert_eval_rows: %s", exc)
            raise
        return

    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)
    try:
        with engine.begin() as conn:
            conn.execute(on_dup)
        logger.info("Upserted %d rows (with %d update cols) for eval_run_tag=%s",
                    len(records), len(update_cols), df_rows.get('eval_run_tag', [None])[0])
    except Exception as exc:
        logger.exception("upsert_eval_rows failed. sample record keys: %s. Error: %s", list(records[0].keys()), exc)
        raise



def upsert_eval_rows_conn(conn: sa.engine.Connection, df_rows: pd.DataFrame):
    """
    Upsert evaluation rows using an existing SQLAlchemy Connection (caller controls tx).
    Sanitizes NaN/NaT/inf values before executing.
    """
    if df_rows is None or df_rows.empty:
        return

    metadata = sa.MetaData()
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=conn.engine)

    records = df_rows.to_dict(orient='records')
    if not records:
        return

    records = _sanitize_records(records)
    insert_columns = set(records[0].keys())
    insert_stmt = sa.dialects.mysql.insert(tbl).values(records)

    writable_tbl_cols = [c for c in tbl.columns if _is_writable_column(c) and c.name in insert_columns]
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in writable_tbl_cols}

    if not update_cols:
        logger.warning("upsert_eval_rows_conn: no writable columns in payload; performing plain INSERT (may fail on duplicates).")
        conn.execute(insert_stmt)
        return

    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)
    try:
        conn.execute(on_dup)
    except Exception as exc:
        logger.exception("upsert_eval_rows_conn failed. sample record keys: %s. Error: %s", list(records[0].keys()), exc)
        raise


def get_unprocessed_trade_dates(engine: sa.engine.Engine, eval_run_tag: str) -> List[date]:
    """
    Return sorted list of trade_date values present in intraday_bhavcopy but not yet recorded
    in signal_evaluation_results for the given eval_run_tag.

    - Uses detect_intraday_columns(engine) to find the actual trade_date column name.
    - Returns list[datetime.date] sorted ascending.
    - Robust to SQLAlchemy 'mappings' vs tuple rows.
    """
    if not eval_run_tag:
        raise ValueError("eval_run_tag must be provided")

    # detect actual column names
    try:
        colmap = detect_intraday_columns(engine)
    except Exception as exc:
        logger.exception("Failed to detect intraday_bhavcopy columns: %s", exc)
        raise

    date_col = colmap.get("trade_date")
    if not date_col:
        raise RuntimeError("Could not detect trade_date column in intraday_bhavcopy")

    # Use LEFT JOIN to find dates in intraday_bhavcopy not present for this eval_run_tag
    sql = sa.text(f"""
        SELECT DISTINCT t.{date_col} AS trade_date
        FROM intraday_bhavcopy t
        LEFT JOIN (
            SELECT DISTINCT trade_date FROM signal_evaluation_results WHERE eval_run_tag = :tag
        ) s ON t.{date_col} = s.trade_date
        WHERE s.trade_date IS NULL
        ORDER BY t.{date_col}
    """)

    dates = []
    with engine.connect() as conn:
        result = conn.execute(sql, {"tag": eval_run_tag})
        try:
            rows = result.mappings().all()  # preferred; returns list of dict-like
            for r in rows:
                # handle if value is already a date or a datetime-like
                dtval = r.get("trade_date")
                if dtval is None:
                    continue
                # normalize to python date
                dt = pd.to_datetime(dtval).date()
                dates.append(dt)
        except Exception:
            # fallback for older SQLAlchemy row objects
            rows = result.fetchall()
            for r in rows:
                if not r:
                    continue
                dtval = r[0]
                if dtval is None:
                    continue
                dt = pd.to_datetime(dtval).date()
                dates.append(dt)

    # remove duplicates and sort ascending
    unique_sorted = sorted(set(dates))
    logger.info("Found %d unprocessed trade_date(s) for tag=%s", len(unique_sorted), eval_run_tag)
    return unique_sorted