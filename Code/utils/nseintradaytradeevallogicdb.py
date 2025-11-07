
import sqlalchemy as sa
import pandas as pd
from typing import List
from datetime import date

def fetch_signals_for_date(engine: sa.engine.Engine, trade_date: date) -> pd.DataFrame:
    """
    Fetch strategy_signals for a single trade_date where entry_model='open'.
    """
    sql = sa.text("""
        SELECT id as signal_id, symbol, signal_date, strategy, entry_model,
               entry_price, stop_price, target_price
        FROM strategy_signals
        WHERE entry_model = 'open' AND signal_date = :d
    """)
    return pd.read_sql(sql, engine, params={"d": trade_date.isoformat()})


def fetch_bhavcopy_on_dates(engine: sa.engine.Engine, symbols_dates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch intraday_bhavcopy OHLC rows for the given (symbol, signal_date) pairs.
    symbols_dates_df must have columns ['symbol', 'signal_date'].
    """
    pairs = symbols_dates_df[['symbol', 'signal_date']].drop_duplicates()
    if pairs.empty:
        return pd.DataFrame()
    dates = sorted(pairs['signal_date'].unique())
    symbols = pairs['symbol'].unique().tolist()

    sql = sa.text("""
        SELECT symbol, trade_date, open_price as open, high_price as high, low_price as low, close_price as close
        FROM intraday_bhavcopy
        WHERE trade_date IN :dates AND symbol IN :symbols
    """)
    df = pd.read_sql(sql, engine, params={"dates": tuple(dates), "symbols": tuple(symbols)})
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
    - conn: SQLAlchemy Connection (from engine.connect())
    - df_rows: DataFrame ready for insertion (same schema as signal_evaluation_results)
    """
    if df_rows is None or df_rows.empty:
        # nothing to do
        return

    metadata = sa.MetaData()
    # autoload using the connection's engine
    tbl = sa.Table('signal_evaluation_results', metadata, autoload_with=conn.engine)
    insert_stmt = sa.dialects.mysql.insert(tbl).values(df_rows.to_dict(orient='records'))
    update_cols = {c.name: insert_stmt.inserted[c.name] for c in tbl.columns if c.name not in ('eval_id',)}
    on_dup = insert_stmt.on_duplicate_key_update(**update_cols)
    # Execute using the provided connection (no begin/commit here)
    conn.execute(on_dup)
    # DO NOT commit here — caller controls transaction


def get_unprocessed_trade_dates(engine: sa.engine.Engine, eval_run_tag: str) -> List[date]:
    """
    Return sorted list of trade_date present in intraday_bhavcopy but not yet processed
    for eval_run_tag in signal_evaluation_results.
    """
    sql = sa.text("""
        SELECT DISTINCT t.trade_date
        FROM intraday_bhavcopy t
        LEFT JOIN (
            SELECT DISTINCT signal_date FROM signal_evaluation_results WHERE eval_run_tag = :tag
        ) s ON t.trade_date = s.signal_date
        WHERE s.signal_date IS NULL
        ORDER BY t.trade_date
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"tag": eval_run_tag}).fetchall()
    return [pd.to_datetime(r['trade_date']).date() for r in rows]
