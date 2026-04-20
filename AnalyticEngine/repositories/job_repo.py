#analytic_engine/repositories/job_repo.py
from AnalyticEngine.utils.db_connection import get_db_connection
from AnalyticEngine.utils.db_schemas import ML_SCHEMA
from datetime import datetime

from sqlalchemy import text


def create_job(trade_date,execution_id):
    """
    Create a new job entry.

    Returns:
        execution_id (str)
    """

    engine = get_db_connection()

    
    now = datetime.utcnow()

    query = f"""
        INSERT INTO {ML_SCHEMA}.ml_job_tracker (
            execution_id,
            trade_date,
            status,
            start_time,
            last_updated_at
        )
        VALUES (:execution_id, :trade_date, :status, :start_time, :last_updated_at)
    """

    with engine.begin() as conn:
        conn.execute(
            text(query),
            {
                "execution_id": execution_id,
                "trade_date": trade_date,
                "status": "PENDING",
                "start_time": now,
                "last_updated_at": now
            }
        )

    return execution_id


def update_job_status(execution_id, status):
    """
    Update job status.

    Args:
        execution_id (str)
        status (str): PENDING / RUNNING / COMPLETED / PARTIAL / FAILED
    """

    engine = get_db_connection()

    now = datetime.utcnow()

    query = f"""
        UPDATE {ML_SCHEMA}.ml_job_tracker
        SET status = :status,
            last_updated_at = :last_updated_at
        WHERE execution_id = :execution_id
    """

    with engine.begin() as conn:
        conn.execute(
            text(query),
            {
                "status": status,
                "last_updated_at": now,
                "execution_id": execution_id
            }
        )


def complete_job(execution_id, status):
    """
    Mark job as completed / partial / failed.

    Args:
        execution_id (str)
        status (str): COMPLETED / PARTIAL / FAILED
    """

    engine = get_db_connection()

    now = datetime.utcnow()

    query = f"""
        UPDATE {ML_SCHEMA}.ml_job_tracker
        SET status = :status,
            end_time = :end_time,
            last_updated_at = :last_updated_at
        WHERE execution_id = :execution_id
    """

    with engine.begin() as conn:
        conn.execute(
            text(query),
            {
                "status": status,
                "end_time": now,
                "last_updated_at": now,
                "execution_id": execution_id
            }
        )


def get_running_job(trade_date):
    """
    Check if a job is already running for a trade_date.

    Returns:
        execution_id or None
    """

    engine = get_db_connection()

    query = f"""
        SELECT execution_id
        FROM {ML_SCHEMA}.ml_job_tracker
        WHERE trade_date = :trade_date
          AND status = 'RUNNING'
        LIMIT 1
    """

    with engine.connect() as conn:
        result = conn.execute(
            text(query),
            {"trade_date": trade_date}
        )
        row = result.fetchone()

    return row[0] if row else None