#Analytic_engine/services/data_loader.py
from AnalyticEngine.repositories.ml_repo import (
    get_nifty_data,
    get_stock_data,
    get_step1_data,
    get_step2_data,
    get_step3_data
)


def load_data(trade_date, logger):
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

    logger.info(f"STEP: Data Loading started | trade_date={trade_date}")

    try:
        logger.info("STEP: Fetching NIFTY data")
        nifty_data = get_nifty_data(trade_date)

        logger.info("STEP: Fetching STOCK data")
        stock_data = get_stock_data(trade_date)

        logger.info("STEP: Fetching STEP1 data")
        step1_data = get_step1_data(trade_date)

        logger.info("STEP: Fetching STEP2 data")
        step2_data = get_step2_data(trade_date)

        logger.info("STEP: Fetching STEP3 data")
        step3_data = get_step3_data(trade_date)

        data_bundle = {
            "nifty_data": nifty_data,
            "stock_data": stock_data,
            "step1_data": step1_data,
            "step2_data": step2_data,
            "step3_data": step3_data
        }

        logger.info(
            f"STEP: Data Loading completed | "
            f"nifty_records={len(nifty_data) if nifty_data else 0} | "
            f"stocks={len(stock_data) if stock_data else 0} | "
            f"step3_candidates={len(step3_data) if step3_data else 0}"
        )

        return data_bundle

    except Exception as e:
        logger.error(f"STEP: Data Loading failed | error={str(e)}")
        return None