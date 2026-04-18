#AnalyticEngine/repositories/nifty_repo.py
from AnalyticEngine.utils.db_connection import get_db_connection
from AnalyticEngine.utils.db_schemas import NIFTY_SCHEMA
from sqlalchemy import text


def get_nifty_candle_count(trade_date):
    """
    Returns the number of NIFTY intraday candles for a given trade_date.

    Used for:
    - NIFTY data completeness validation in trade_date_resolver

    Args:
        trade_date (str): Trade date (YYYY-MM-DD)

    Returns:
        int: Number of 5-minute candles available
    """

    engine = get_db_connection()

    query =f"""
        SELECT COUNT(1)
        FROM {NIFTY_SCHEMA}.nifty_prices
        WHERE DATE(Date) = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] if row else 0