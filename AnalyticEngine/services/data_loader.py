#AnalyticEngine/services/data_loader.py
from AnalyticEngine.repositories.ml_repo import (
    get_nifty_data,
    get_stock_data,
    get_step1_data,
    get_step2_data,
    get_step3_data
)

from AnalyticEngine.utils.logger import get_logger


logger = get_logger(__name__)


def load_data(trade_date):
    """
    Data Loader

    INPUT:
        trade_date (str)

    PROCESS:
        - Fetch all required data from repositories
        - No transformation
        - No validation (handled separately)

    OUTPUT:
        dict:
        {
            "nifty_data": list,
            "stock_data": dict,
            "step1_data": dict,
            "step2_data": dict,
            "step3_data": list
        }
    """

    logger.info(f"Loading data for trade_date: {trade_date}")

    try:
        nifty_data = get_nifty_data(trade_date)
        stock_data = get_stock_data(trade_date)
        step1_data = get_step1_data(trade_date)
        step2_data = get_step2_data(trade_date)
        step3_data = get_step3_data(trade_date)

        data_bundle = {
            "nifty_data": nifty_data,
            "stock_data": stock_data,
            "step1_data": step1_data,
            "step2_data": step2_data,
            "step3_data": step3_data
        }

        logger.info("Data loading completed")

        return data_bundle

    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        return None