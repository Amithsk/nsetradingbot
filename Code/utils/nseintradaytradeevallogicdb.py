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

# Ensure project root on path so relative imports work
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from Code.utils.nseintraday_db_utils import detect_intraday_columns

logger = logging.getLogger("nseintradaytradeevallogicdb")

# engine-object based cache
_colmap_cache_by_engine = {}


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
    Expects unique key (signal_id, eval_run_tag) on the table.
    """
    if df_rows is None or df_rows.empty:
        logger.info("upsert_eval_rows: nothing to upsert.")
        return

    metadata = sa.MetaData()
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=engine)
    insert_stmt = sa.dialects.mysql.insert(tbl).values(df_rows.to_dict(orient='records'))
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in tbl.columns if c.name not in ('eval_id',)}
    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)
    with engine.begin() as conn:
        conn.execute(on_dup)
    logger.info("Upserted %d rows for eval_run_tag=%s", len(df_rows), df_rows['eval_run_tag'].iloc[0])


def upsert_eval_rows_conn(conn: sa.engine.Connection, df_rows: pd.DataFrame):
    """
    Upsert evaluation rows using an existing SQLAlchemy Connection.
    Caller manages transaction.
    """
    if df_rows is None or df_rows.empty:
        return
    metadata = sa.MetaData()
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=conn.engine)
    insert_stmt = sa.dialects.mysql.insert(tbl).values(df_rows.to_dict(orient='records'))
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in tbl.columns if c.name not in ('eval_id',)}
    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)
    conn.execute(on_dup)


def get_unprocessed_trade_dates(engine: sa.engine.Engine, eval_run_tag: str) -> List[date]:
    """
    Return sorted list of trade_date present in intraday_bhavcopy but not yet processed
    for eval_run_tag in signal_evaluation_results.
    """
    colmap = _get_colmap_for_engine(engine)
    date_col = colmap.get('trade_date')
    if not date_col:
        raise RuntimeError("Could not detect trade_date column in intraday_bhavcopy (for get_unprocessed_trade_dates).")

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
