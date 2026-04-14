#AnalyticEngine/utils/logger.py
import logging
import os
from datetime import datetime


LOG_DIR = "logs"


def _ensure_log_directory():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)


def _get_log_file_name(execution_id: str):
    date_str = datetime.now().strftime("%Y%m%d")
    return os.path.join(LOG_DIR, f"engine_{date_str}_{execution_id}.log")


def get_logger(execution_id: str):
    """
    Returns a configured logger for a given execution.

    Logging Requirements:
    - Every execution must be fully traceable
    - Logs must be tied to execution_id
    - All steps must log status (resolution, validation, modules, storage)

    Args:
        execution_id (str): Unique ID for the execution job

    Returns:
        logger (logging.Logger): Configured logger instance
    """

    _ensure_log_directory()

    logger_name = f"AnalyticEngine_{execution_id}"
    logger = logging.getLogger(logger_name)

    # Prevent duplicate handlers (important for re-runs)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    log_file = _get_log_file_name(execution_id)

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger