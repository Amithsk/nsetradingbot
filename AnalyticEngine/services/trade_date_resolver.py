#AnalyticEngine/services/trade_date_resolver.py
from AnalyticEngine.repositories.step_repo import (
    get_available_trade_dates,
    check_step3_execution_exists,
    get_step3_stock_count
)

from AnalyticEngine.repositories.stock_repo import (
    get_instruments_count,
    get_stock_data_status  # NEW (you will add this repo method)
)

from AnalyticEngine.repositories.nifty_repo import (
    get_nifty_candle_count  # NEW (you will add this repo method)
)

from AnalyticEngine.utils.logger import get_logger

logger = get_logger(__name__)


def resolve_trade_date():
    """
    Resolve the latest valid trade_date based on data completeness.

    RULES:
    1. step3_execution_control must exist
    2. step3_stock_selection must exist
    3. step3_stock_selection count >= instruments_master count
    4. NIFTY data must be complete (>= 75 candles)
    5. stock_data_status must be FINAL

    RETURNS:
        trade_date (str)
        resolution_log (list)
    """

    resolution_log = []

    # --------------------------------------
    # Fetch all candidate trade_dates (DESC)
    # --------------------------------------
    trade_dates = get_available_trade_dates()

    if not trade_dates:
        logger.error("No trade_dates found in step3_execution_control")
        raise Exception("Trade date resolution failed: No available dates")

    # --------------------------------------
    # Get total instruments count (universe)
    # --------------------------------------
    total_instruments = get_instruments_count()
    logger.info(f"Total instruments in universe: {total_instruments}")

    # --------------------------------------
    # Iterate over trade_dates (latest first)
    # --------------------------------------
    for trade_date in trade_dates:

        log_entry = {
            "trade_date": trade_date,
            "step3_execution": False,
            "stock_count": 0,
            "instrument_count": total_instruments,
            "nifty_candles": 0,
            "stock_status": None,
            "status": "REJECTED",
            "reason": None
        }

        # --------------------------------------
        # STEP 3 execution check
        # --------------------------------------
        step3_exec_exists = check_step3_execution_exists(trade_date)

        if not step3_exec_exists:
            log_entry["reason"] = "Missing step3_execution_control"
            resolution_log.append(log_entry)
            continue

        log_entry["step3_execution"] = True

        # --------------------------------------
        # STEP 3 stock selection count
        # --------------------------------------
        stock_count = get_step3_stock_count(trade_date)
        log_entry["stock_count"] = stock_count

        if stock_count == 0:
            log_entry["reason"] = "No records in step3_stock_selection"
            resolution_log.append(log_entry)
            continue

        if stock_count < total_instruments:
            log_entry["reason"] = "Incomplete stock coverage"
            resolution_log.append(log_entry)
            continue

        # --------------------------------------
        # NIFTY completeness check (NEW)
        # --------------------------------------
        nifty_count = get_nifty_candle_count(trade_date)
        log_entry["nifty_candles"] = nifty_count

        if nifty_count < 75:
            log_entry["reason"] = "Incomplete NIFTY data"
            resolution_log.append(log_entry)
            continue

        # --------------------------------------
        # STOCK FINAL status check (NEW)
        # --------------------------------------
        stock_status = get_stock_data_status(trade_date)
        log_entry["stock_status"] = stock_status

        if stock_status != "FINAL":
            log_entry["reason"] = "Stock data not FINAL"
            resolution_log.append(log_entry)
            continue

        # --------------------------------------
        # VALID TRADE DATE FOUND
        # --------------------------------------
        log_entry["status"] = "SELECTED"
        log_entry["reason"] = "All validation checks passed"

        resolution_log.append(log_entry)

        logger.info(f"Resolved trade_date: {trade_date}")

        return trade_date, resolution_log

    # --------------------------------------
    # No valid trade_date found
    # --------------------------------------
    logger.error("No valid trade_date found after validation")

    raise Exception("Trade date resolution failed: No valid date found")