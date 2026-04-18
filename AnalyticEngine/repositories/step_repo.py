# AnalyticalEngine/repositories/step_repo.py

from AnalyticEngine.utils.db_connection import get_db_connection
from AnalyticEngine.utils.logger import get_logger

logger = get_logger(__name__)


def get_available_trade_dates():
    """
    Fetch all distinct trade_dates from step3_execution_control
    ordered descending (latest first)
    """
    query = """
        SELECT DISTINCT trade_date
        FROM intradaytrading.step3_execution_control
        ORDER BY trade_date DESC
    """

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        results = cursor.fetchall()

        trade_dates = [row[0] for row in results]

        logger.info(f"Fetched {len(trade_dates)} trade_dates from step3_execution_control")

        return trade_dates

    except Exception as e:
        logger.error(f"Error fetching trade_dates: {str(e)}")
        raise

    finally:
        cursor.close()
        connection.close()


def check_step3_execution_exists(trade_date):
    """
    Check if step3_execution_control exists for given trade_date
    """
    query = """
        SELECT 1
        FROM intradaytrading.step3_execution_control
        WHERE trade_date = %s
        LIMIT 1
    """

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(query, (trade_date,))
        result = cursor.fetchone()

        exists = result is not None

        logger.debug(f"step3_execution_control exists for {trade_date}: {exists}")

        return exists

    except Exception as e:
        logger.error(f"Error checking step3_execution_control for {trade_date}: {str(e)}")
        raise

    finally:
        cursor.close()
        connection.close()


def get_step3_stock_count(trade_date):
    """
    Get count of records in step3_stock_selection for given trade_date
    """
    query = """
        SELECT COUNT(*)
        FROM intradaytrading.step3_stock_selection
        WHERE trade_date = %s
    """

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(query, (trade_date,))
        result = cursor.fetchone()

        count = result[0] if result else 0

        logger.debug(f"Stock count for {trade_date}: {count}")

        return count

    except Exception as e:
        logger.error(f"Error fetching stock count for {trade_date}: {str(e)}")
        raise

    finally:
        cursor.close()
        connection.close()

