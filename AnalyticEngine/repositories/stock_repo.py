# AnalyticalEngine/repositories/stock_repo.py
from AnalyticEngine.utils.db_connection import get_db_connection
from AnalyticEngine.utils.logger import get_logger
from sqlalchemy import text

logger = get_logger(__name__)


def get_instruments_count():
    """
    Get total number of instruments from instruments_master.
    This represents the full stock universe.
    """
    query = """
        SELECT COUNT(1)
        FROM intradaytrading.instruments_master
    """

    engine = get_db_connection()

    with engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()

    count = row[0] if row else 0

    logger.info(f"Total instruments count: {count}")

    return count


def get_stock_data_status(trade_date):
    """
    Returns stock data finalization status for a given trade_date.

    Expected values:
    - FINAL
    - PARTIAL
    """
    query = """
        SELECT status
        FROM intradaytrading.stock_data_status
        WHERE trade_date = :trade_date
    """

    engine = get_db_connection()

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    status = row[0] if row else None

    logger.debug(f"Stock data status for {trade_date}: {status}")

    return status