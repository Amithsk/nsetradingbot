#AnalyticEngine/repositories/step_repo.py
from AnalyticEngine.utils.db_connection import get_db_connection
from sqlalchemy import text


def get_available_trade_dates():
    """
    Fetch all available trade_dates from step3_execution_control (latest first).

    Returns:
        list[str]: trade_dates sorted DESC
    """

    engine = get_db_connection()

    query = """
        SELECT DISTINCT trade_date
        FROM step3_execution_control
        ORDER BY trade_date DESC
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        results = result.fetchall()

    return [row[0] for row in results]


def check_step1_exists(trade_date):
    """
    Check if STEP 1 output exists for a given trade_date.
    """

    engine = get_db_connection()

    query = """
        SELECT COUNT(1)
        FROM step1_output
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] > 0 if row else False


def check_step2_exists(trade_date):
    """
    Check if STEP 2 output exists for a given trade_date.
    """

    engine = get_db_connection()

    query = """
        SELECT COUNT(1)
        FROM step2_output
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] > 0 if row else False


def check_step3_execution_exists(trade_date):
    """
    Check if STEP 3 execution control exists for a given trade_date.
    """

    engine = get_db_connection()

    query = """
        SELECT COUNT(1)
        FROM step3_execution_control
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] > 0 if row else False


def get_step3_stock_count(trade_date):
    """
    Returns number of records in step3_stock_selection for a given trade_date.
    """

    engine = get_db_connection()

    query = """
        SELECT COUNT(1)
        FROM step3_stock_selection
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] if row else 0