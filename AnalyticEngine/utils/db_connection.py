#AnalyticEngine/utils/db_connection.py
from Code.utils.nseintraday_db_utils import connect_db


def get_db_connection():
    """
    Provides a database connection for the Analytical Engine.

    This is a thin wrapper over the existing DB utility to ensure:
    - No duplication of DB logic
    - Centralized connection management
    - Compliance with architecture (Data Layer reuse)

    Returns:
        connection: Active DB connection object
    """
    return connect_db()