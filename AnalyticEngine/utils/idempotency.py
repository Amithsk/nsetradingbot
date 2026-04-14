#AnalyticEngine/utils/idempotency.py
from utils.db_connection import get_db_connection


def delete_existing_nifty_insights(trade_date):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        DELETE FROM ml_nifty_insights
        WHERE trade_date = %s
    """
    cursor.execute(query, (trade_date,))
    conn.commit()

    cursor.close()
    conn.close()


def delete_existing_stock_insights(trade_date):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        DELETE FROM ml_stock_insights
        WHERE trade_date = %s
    """
    cursor.execute(query, (trade_date,))
    conn.commit()

    cursor.close()
    conn.close()


def delete_existing_stock_diagnostics(trade_date):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        DELETE FROM ml_stock_diagnostics
        WHERE trade_date = %s
    """
    cursor.execute(query, (trade_date,))
    conn.commit()

    cursor.close()
    conn.close()


def delete_existing_suggestions(trade_date):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        DELETE FROM ml_suggestions
        WHERE trade_date = %s
    """
    cursor.execute(query, (trade_date,))
    conn.commit()

    cursor.close()
    conn.close()


def delete_existing_summary(trade_date):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        DELETE FROM ml_summary
        WHERE trade_date = %s
    """
    cursor.execute(query, (trade_date,))
    conn.commit()

    cursor.close()
    conn.close()


def cleanup_existing_outputs(trade_date):
    """
    Ensures idempotent execution by removing existing outputs
    for the given trade_date before reprocessing.

    This guarantees:
    - No duplicate records
    - Safe re-run capability
    - Clean overwrite of outputs
    """
    delete_existing_nifty_insights(trade_date)
    delete_existing_stock_insights(trade_date)
    delete_existing_stock_diagnostics(trade_date)
    delete_existing_suggestions(trade_date)
    delete_existing_summary(trade_date)