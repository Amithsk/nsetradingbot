# Code/utils/nseintradaytradeevallogicdb.py
import sqlalchemy as sa
import pandas as pd
from typing import List
from datetime import date

def fetch_signals_for_date(engine: sa.engine.Engine, trade_date: date) -> pd.DataFrame:
    """
    Fetch strategy_signals for a single trade_date where entry_model='open'.
    Returns a pandas DataFrame using pd.read_sql (keeps original behavior).
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
    Fetch intraday_bhavcopy OHLC rows for the given (symbol, signal_date) pairs.
    This version auto-detects column names for OHLC/trade_date and aliases them to:
      symbol, trade_date, open, high, low, close

    symbols_dates_df must have columns ['symbol', 'signal_date'].
    """
    pairs = symbols_dates_df[['symbol', 'signal_date']].drop_duplicates()
    if pairs.empty:
        return pd.DataFrame()

    # Candidate names for the columns (common variants)
    ohlc_candidates = {
        'open': ['open_price', 'open'],
        'high': ['high_price', 'high'],
        'low':  ['low_price', 'low'],
        'close':['close_price', 'close']
    }
    trade_date_candidates = ['trade_date', 'tradeDate', 'trade_day']

    # inspect table to find actual column names
    inspector = sa.inspect(engine)
    try:
        cols_info = inspector.get_columns('intraday_bhavcopy')
        actual_cols = {c['name'] for c in cols_info}
    except Exception:
        # fallback to information_schema
        q = sa.text("""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'intraday_bhavcopy'
        """)
        with engine.connect() as conn:
            res = conn.execute(q)
            actual_cols = {row[0] for row in res.fetchall()}

    def find_col(candidates):
        for cand in candidates:
            if cand in actual_cols:
                return cand
        return None

    open_col = find_col(ohlc_candidates['open'])
    high_col = find_col(ohlc_candidates['high'])
    low_col  = find_col(ohlc_candidates['low'])
    close_col= find_col(ohlc_candidates['close'])
    date_col = find_col(trade_date_candidates)

    missing = []
    if not open_col: missing.append('open')
    if not high_col: missing.append('high')
    if not low_col: missing.append('low')
    if not close_col: missing.append('close')
    if not date_col: missing.append('trade_date')

    if missing:
        raise RuntimeError(f"Could not detect required intraday_bhavcopy columns: {missing}. "
                           "Run SHOW COLUMNS FROM intraday_bhavcopy and adapt candidates or rename columns.")

    # Build SELECT with aliases so output columns are normalized
    dates = sorted(pairs['signal_date'].unique())
    symbols = pairs['symbol'].unique().tolist()

    sql = sa.text(f"""
        SELECT symbol,
               {date_col} AS trade_date,
               {open_col}  AS open,
               {high_col}  AS high,
               {low_col}   AS low,
               {close_col} AS close
        FROM intraday_bhavcopy
        WHERE {date_col} IN :dates AND symbol IN :symbols
    """)

    df = pd.read_sql(sql, engine, params={"dates": tuple(dates), "symbols": tuple(symbols)})
    if not df.empty:
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    return df



def upsert_eval_rows(engine: sa.engine.Engine, df_rows: pd.DataFrame):
    """
    Upsert evaluation result rows into signal_evaluation_results using ON DUPLICATE KEY UPDATE.
    Expects unique key (signal_id, eval_run_tag) on the table.
    This function manages its own transaction (engine.begin()) — normal commit behavior.
    """
    if df_rows is None or df_rows.empty:
        print("upsert_eval_rows: nothing to upsert.")
        return
    metadata = sa.MetaData()
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=engine)
    insert_stmt = sa.dialects.mysql.insert(tbl).values(df_rows.to_dict(orient='records'))
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in tbl.columns if c.name not in ('eval_id', )}
    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)
    with engine.begin() as conn:
        conn.execute(on_dup)
    print(f"Upserted {len(df_rows)} rows for eval_run_tag={df_rows['eval_run_tag'].iloc[0]}")


def upsert_eval_rows_conn(conn: sa.engine.Connection, df_rows: pd.DataFrame):
    """
    Upsert evaluation rows using an existing SQLAlchemy Connection.
    Caller is responsible for transaction (begin/commit/rollback).
    Use this in transaction-preview (rollback) mode.
    """
    if df_rows is None or df_rows.empty:
        return

    metadata = sa.MetaData()
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=conn.engine)
    insert_stmt = sa.dialects.mysql.insert(tbl).values(df_rows.to_dict(orient='records'))
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in tbl.columns if c.name not in ('eval_id',)}
    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)
    # Execute using the provided connection (no begin/commit here)
    conn.execute(on_dup)
    # caller controls transaction/commit/rollback


def get_unprocessed_trade_dates(engine: sa.engine.Engine, eval_run_tag: str) -> List[date]:
    """
    Return sorted list of trade_date present in intraday_bhavcopy but not yet processed
    for eval_run_tag in signal_evaluation_results.
    Robust to SQLAlchemy result row types (mapping vs tuple).
    """
    sql = sa.text("""
        SELECT DISTINCT t.trade_date
        FROM intraday_bhavcopy t
        LEFT JOIN (
            SELECT DISTINCT trade_date FROM signal_evaluation_results WHERE eval_run_tag = :tag
        ) s ON t.trade_date = s.trade_date
        WHERE s.trade_date IS NULL
        ORDER BY t.trade_date
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"tag": eval_run_tag})
        try:
            rows = result.mappings().all()
            dates = [pd.to_datetime(r['trade_date']).date() for r in rows]
        except Exception:
            rows = result.fetchall()
            # fallback: assume first column is trade_date
            dates = [pd.to_datetime(r[0]).date() for r in rows]
    return dates
