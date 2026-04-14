# AnalyticEngine/services/validation_engine.py
from AnalyticEngine.repositories.step_repo import (
    check_step1_exists,
    check_step2_exists,
    check_step3_execution_exists
)

from AnalyticEngine.repositories.stock_repo import (
    get_stock_data_status
)

from AnalyticEngine.repositories.nifty_repo import (
    get_nifty_candle_count
)

from AnalyticEngine.utils.logger import get_logger


logger = get_logger(__name__)


def validate_inputs(trade_date):
    """
    Validates all required inputs before execution.

    VALIDATION SCOPE:
    - STEP 1 exists
    - STEP 2 exists
    - STEP 3 exists
    - NIFTY data completeness
    - STOCK data FINAL status

    RETURNS:
        validation_status (str): COMPLETE / PARTIAL / SKIPPED
        validation_log (dict)
    """

    validation_log = {
        "trade_date": trade_date,
        "step1": False,
        "step2": False,
        "step3": False,
        "nifty_candles": 0,
        "nifty_complete": False,
        "stock_status": None,
        "stock_final": False,
        "final_status": None
    }

    logger.info(f"Starting validation for trade_date: {trade_date}")

    # --------------------------------------
    # STEP VALIDATION
    # --------------------------------------
    step1_exists = check_step1_exists(trade_date)
    step2_exists = check_step2_exists(trade_date)
    step3_exists = check_step3_execution_exists(trade_date)

    validation_log["step1"] = step1_exists
    validation_log["step2"] = step2_exists
    validation_log["step3"] = step3_exists

    # --------------------------------------
    # NIFTY VALIDATION
    # --------------------------------------
    nifty_count = get_nifty_candle_count(trade_date)
    validation_log["nifty_candles"] = nifty_count

    if nifty_count >= 75:
        validation_log["nifty_complete"] = True

    # --------------------------------------
    # STOCK VALIDATION
    # --------------------------------------
    stock_status = get_stock_data_status(trade_date)
    validation_log["stock_status"] = stock_status

    if stock_status == "FINAL":
        validation_log["stock_final"] = True

    # --------------------------------------
    # FINAL VALIDATION STATUS
    # --------------------------------------
    if (
        validation_log["step1"]
        and validation_log["step2"]
        and validation_log["step3"]
        and validation_log["nifty_complete"]
        and validation_log["stock_final"]
    ):
        final_status = "COMPLETE"

    elif (
        validation_log["step3"]
        and (validation_log["nifty_complete"] or validation_log["stock_final"])
    ):
        final_status = "PARTIAL"

    else:
        final_status = "SKIPPED"

    validation_log["final_status"] = final_status

    logger.info(f"Validation result: {validation_log}")

    return final_status, validation_log