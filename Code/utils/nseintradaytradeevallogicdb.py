
import logging
from functools import lru_cache
from datetime import date
from typing import List

import pandas as pd
import sqlalchemy as sa

from pathlib import Path
import sys
# --- ensure project root is importable for Code.utils.*
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from Code.utils.nseintraday_db_utils import detect_intraday_columns  # centralized detector

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _cached_detect_intraday_columns(db_url: str) -> dict:
    """
    Cache detection per-process. We cache on DB URL (string) so multiple engine objects
    pointing to same DB share the cached result. Caller provides engine.url.render_as_string().
    """
    # NOTE: this function expects a DB URL string; callers wrap it below.
    # We will re-create an engine for detection to avoid requiring the caller to pass engine every time
    temp_engine = sa.create_engine(db_url)
    try:
        colmap = detect_intraday_columns(temp_engine)
    finally:
        temp_engine.dispose()
    return colmap


def _get_colmap_for_engine(engine: sa.engine.Engine) -> dict:
    """
    Return detected column mapping for provided SQLAlchemy engine.
    Uses an internal cache keyed by engine.url string.
    """
    try:
        db_url = engine.url.render_as_string(hide_password=True)
    except Exception:
        # Fallback: use object's repr if render_as_string fails
        db_url = repr(engine)
    return _cached_detect_intraday_columns(db_url)


def fetch_signals_for_date(engine: sa.engine.Engine, trade_date: date) -> pd.DataFrame:
    """
    Fetch strategy_signals for a single trade_date where entry_model='open'.
    Returns a pandas DataFrame.
    """
    sql = sa.text("""
        SELECT id AS signal_id,
               symbol,
               trade_date,
               strategy,
               entry_model,
               entry_price,
               stop_price,
               target_price
        FROM strategy_signals
        WHERE entry_model = 'open' AND trade_date = :d
    """)
    # SQLAlchemy can handle a date object for :d
    df = pd.read_sql(sql, engine, params={"d": trade_date})
    return df


def fetch_bhavcopy_on_dates(engine: sa.engine.Engine, symbols_dates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch intraday_bhavcopy OHLC rows for the given (symbol, trade_date) pairs.

    symbols_dates_df must contain columns ['symbol', 'trade_date'].
    Output columns are normalized to: symbol, trade_date (date), open, high, low, close
    """
    if symbols_dates_df is None:
        return pd.DataFrame()

    if not {'symbol', 'trade_date'}.issubset(set(symbols_dates_df.columns)):
        raise ValueError("symbols_dates_df must contain 'symbol' and 'trade_date' columns")

    pairs = symbols_dates_df[['symbol', 'trade_date']].drop_duplicates()
    if pairs.empty:
        return pd.DataFrame()

    # Detect actual intraday_bhavcopy column names (cached)
    colmap = _get_colmap_for_engine(engine)
    required = ['symbol', 'trade_date', 'open', 'high', 'low', 'close']
    missing = [k for k in required if k not in colmap or not colmap[k]]
    if missing:
        raise RuntimeError(
            f"detect_intraday_columns() did not return required keys: {missing}. "
            "Please update the detector candidate lists or rename DB columns."
        )

    symbol_col = colmap['symbol']
    date_col = colmap['trade_date']
    open_col = colmap['open']
    high_col = colmap['high']
    low_col = colmap['low']
    close_col = colmap['close']

    # normalize lists for params
    # ensure dates are date objects (or strings) in a normalized form
    dates = sorted(pd.to_datetime(pairs['trade_date']).dt.date.unique().tolist())
    symbols = list(pairs['symbol'].unique())

    if not dates or not symbols:
        return pd.DataFrame()

    # Build SQL with expanding bindparams to safely expand IN lists
    sql = sa.text(f"""
    SELECT {symbol_col} AS symbol,
           {date_col}    AS trade_date,
           {open_col}    AS open,
           {high_col}    AS high,
           {low_col}     AS low,
           {close_col}   AS close
    FROM intraday_bhavcopy
    WHERE {date_col} IN :dates AND {symbol_col} IN :symbols
    """).bindparams(
        sa.bindparam("dates", expanding=True),
        sa.bindparam("symbols", expanding=True),
    )

    # Use pandas read_sql to get a DataFrame
    df = pd.read_sql(sql, engine, params={"dates": dates, "symbols": symbols})
    if not df.empty:
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    logger.debug("Fetched %d rows from intraday_bhavcopy for %d dates and %d symbols",
                 len(df), len(dates), len(symbols))
    return df


def upsert_eval_rows(engine: sa.engine.Engine, df_rows: pd.DataFrame):
    """
    Upsert evaluation result rows into signal_evaluation_results using ON DUPLICATE KEY UPDATE.
    Expects that signal_evaluation_results has appropriate unique key (eg (signal_id, eval_run_tag)).
    This function manages its own transaction.
    """
    if df_rows is None or df_rows.empty:
        logger.info("upsert_eval_rows: nothing to upsert.")
        return

    if 'eval_run_tag' not in df_rows.columns:
        raise ValueError("df_rows must contain 'eval_run_tag' column")

    metadata = sa.MetaData()
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=engine)
    records = df_rows.to_dict(orient='records')

    insert_stmt = sa.dialects.mysql.insert(tbl).values(records)
    # Build update mapping for all non-primary-key columns.
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in tbl.columns if not c.primary_key}
    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)

    with engine.begin() as conn:
        conn.execute(on_dup)

    logger.info("Upserted %d rows for eval_run_tag=%s", len(df_rows), df_rows['eval_run_tag'].iloc[0])


def upsert_eval_rows_conn(conn: sa.engine.Connection, df_rows: pd.DataFrame):
    """
    Upsert evaluation rows using an existing SQLAlchemy Connection.
    Caller is responsible for transaction (begin/commit/rollback).
    """
    if df_rows is None or df_rows.empty:
        return

    metadata = sa.MetaData()
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=conn.engine)
    records = df_rows.to_dict(orient='records')

    insert_stmt = sa.dialects.mysql.insert(tbl).values(records)
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in tbl.columns if not c.primary_key}
    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)

    conn.execute(on_dup)


def get_unprocessed_trade_dates(engine: sa.engine.Engine, eval_run_tag: str) -> List[date]:
    """
    Return sorted list of trade_date present in intraday_bhavcopy but not yet processed
    for eval_run_tag in signal_evaluation_results.

    Uses detect_intraday_columns to find real date column name.
    """
    colmap = _get_colmap_for_engine(engine)
    date_col = colmap.get('trade_date')
    if not date_col:
        raise RuntimeError("Could not detect trade_date column in intraday_bhavcopy.")

    # Build SQL using actual column name for t.<date_col>
    sql = sa.text(f"""
        SELECT DISTINCT t.{date_col} AS trade_date
        FROM intraday_bhavcopy t
        LEFT JOIN (
            SELECT DISTINCT trade_date FROM signal_evaluation_results WHERE eval_run_tag = :tag
        ) s ON t.{date_col} = s.trade_date
        WHERE s.trade_date IS NULL
        ORDER BY t.{date_col}
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"tag": eval_run_tag})
        try:
            rows = result.mappings().all()
            dates = [pd.to_datetime(r['trade_date']).date() for r in rows]
        except Exception:
            rows = result.fetchall()
            dates = [pd.to_datetime(r[0]).date() for r in rows]
    return dates
