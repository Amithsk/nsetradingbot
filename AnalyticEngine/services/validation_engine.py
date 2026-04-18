#AnalyticEngine/services/validation_engine.py
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


def run_validation(trade_date, logger):
    logger.info(f"STEP: Validation started | trade_date={trade_date}")

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

    # --------------------------------------
    # STEP VALIDATION
    # --------------------------------------
    logger.info("STEP: Checking STEP outputs")

    step1_exists = check_step1_exists(trade_date)
    step2_exists = check_step2_exists(trade_date)
    step3_exists = check_step3_execution_exists(trade_date)

    validation_log["step1"] = step1_exists
    validation_log["step2"] = step2_exists
    validation_log["step3"] = step3_exists

    logger.info(f"STEP1 exists: {step1_exists}")
    logger.info(f"STEP2 exists: {step2_exists}")
    logger.info(f"STEP3 exists: {step3_exists}")

    # --------------------------------------
    # NIFTY VALIDATION
    # --------------------------------------
    logger.info("STEP: Checking NIFTY completeness")

    nifty_count = get_nifty_candle_count(trade_date)
    validation_log["nifty_candles"] = nifty_count

    if nifty_count >= 75:
        validation_log["nifty_complete"] = True
        logger.info(f"NIFTY complete: True | candles={nifty_count}")
    else:
        logger.warning(f"NIFTY incomplete | candles={nifty_count} (<75)")

    # --------------------------------------
    # STOCK VALIDATION
    # --------------------------------------
    logger.info("STEP: Checking STOCK data status")
    

    stock_status = get_stock_data_status(trade_date)
    validation_log["stock_status"] = stock_status

    if stock_status == "FINAL":
        validation_log["stock_final"] = True
        logger.info("Stock data status: FINAL")
    else:
        logger.warning(f"Stock data not FINAL | status={stock_status}")

    # --------------------------------------
    # FINAL VALIDATION STATUS
    # --------------------------------------
    logger.info("STEP: Computing final validation status")

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

    logger.info(f"VALIDATION RESULT: {final_status}")
    logger.info(f"Validation summary: {validation_log}")

    logger.info("STEP: Validation completed")

    return final_status, validation_log