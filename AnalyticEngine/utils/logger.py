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

def setup_execution_logger(execution_id: str = None):
    _ensure_log_directory()

    logger = logging.getLogger("AnalyticEngine")

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    if execution_id:
        log_file = _get_log_file_name(execution_id)
    else:
        # 👇 fallback file
        date_str = datetime.now().strftime("%Y%m%d")
        log_file = os.path.join(LOG_DIR, f"engine_pre_execution_{date_str}.log")

    file_handler = logging.FileHandler(log_file)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str):
    """
    Module-level logger (shared across execution)
    """
    return logging.getLogger("AnalyticEngine." + name)