#AnalyticEngine/repositories/stock_repo.py
from AnalyticEngine.utils.db_connection import get_db_connection
from sqlalchemy import text


def get_instruments_count():
    """
    Returns total number of instruments in the trading universe.

    Used for:
    - Validating STEP 3 stock coverage completeness
    """

    engine = get_db_connection()

    query = """
        SELECT COUNT(1)
        FROM instruments_master
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()

    return row[0] if row else 0


def get_stock_data_status(trade_date):
    """
    Returns stock data finalization status for a given trade_date.

    Expected values:
    - FINAL
    - PARTIAL

    Used for:
    - Ensuring only finalized stock data is used in analysis

    Args:
        trade_date (str): Trade date (YYYY-MM-DD)

    Returns:
        str: status (FINAL / PARTIAL / None)
    """

    engine = get_db_connection()

    query = """
        SELECT status
        FROM stock_data_status
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] if row else None