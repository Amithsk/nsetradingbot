# AnalyticalEngine/repositories/stock_repo.py

from AnalyticalEngine.utils.db_connection import get_db_connection
from AnalyticalEngine.utils.logger import get_logger

logger = get_logger(__name__)


def get_instruments_count():
    """
    Get total number of instruments from instruments_master.
    This represents the full stock universe.
    """
    query = """
        SELECT COUNT(*)
        FROM intradaytrading.instruments_master
    """

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchone()

        count = result[0] if result else 0

        logger.info(f"Total instruments count: {count}")

        return count

    except Exception as e:
        logger.error(f"Error fetching instruments count: {str(e)}")
        raise

    finally:
        cursor.close()
        connection.close()