#AnalyticEngine/repositories/step_repo.py
from AnalyticEngine.utils.db_connection import get_db_connection
from sqlalchemy import text


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

    engine = get_db_connection()

    with engine.connect() as conn:
        result = conn.execute(text(query))
        results = result.fetchall()

    trade_dates = [row[0] for row in results]

    return trade_dates


def check_step1_exists(trade_date):
    """
    Check if STEP 1 output exists for a given trade_date
    """
    query = """
        SELECT COUNT(1)
        FROM intradaytrading.step1_market_context
        WHERE trade_date = :trade_date
    """

    engine = get_db_connection()

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] > 0 if row else False


def check_step2_exists(trade_date):
    """
    Check if STEP 2 output exists for a given trade_date
    """
    query = """
        SELECT COUNT(1)
        FROM intradaytrading.step2_market_open_behavior
        WHERE trade_date = :trade_date
    """

    engine = get_db_connection()

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] > 0 if row else False


def check_step3_execution_exists(trade_date):
    """
    Check if step3_execution_control exists for given trade_date
    """
    query = """
        SELECT 1
        FROM intradaytrading.step3_execution_control
        WHERE trade_date = :trade_date
        LIMIT 1
    """

    engine = get_db_connection()

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row is not None


def get_step3_stock_count(trade_date):
    """
    Get count of records in step3_stock_selection for given trade_date
    """
    query = """
        SELECT COUNT(1)
        FROM intradaytrading.step3_stock_selection
        WHERE trade_date = :trade_date
    """

    engine = get_db_connection()

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    return row[0] if row else 0