from AnalyticEngine.repositories.step_repo import (
    get_available_trade_dates,
    check_step3_execution_exists,
    get_step3_stock_count
)

from AnalyticEngine.repositories.stock_repo import (
    get_instruments_count
)


def resolve_trade_date(logger):
    logger.info("STEP: Trade Date Resolution started")

    resolution_log = []

    # --------------------------------------
    # Fetch trade dates
    # --------------------------------------
    logger.info("STEP: Fetching available trade_dates")
    trade_dates = get_available_trade_dates()

    if not trade_dates:
        logger.error("No trade_dates found in step3_execution_control")
        raise Exception("Trade date resolution failed: No available dates")

    logger.info(f"STEP: Found {len(trade_dates)} trade_dates")

    # --------------------------------------
    # Fetch instrument count
    # --------------------------------------
    logger.info("STEP: Fetching instrument universe size")
    total_instruments = get_instruments_count()
    logger.info(f"Total instruments in universe: {total_instruments}")

    # --------------------------------------
    # Iterate trade dates
    # --------------------------------------
    for trade_date in trade_dates:

        logger.info(f"CHECKING trade_date={trade_date}")

        log_entry = {
            "trade_date": trade_date,
            "step3_execution": False,
            "stock_count": 0,
            "instrument_count": total_instruments,
            "status": "REJECTED",
            "reason": None
        }

        # --------------------------------------
        # STEP 3 execution check
        # --------------------------------------
        logger.info(f"STEP: Checking step3_execution_control for {trade_date}")
        step3_exec_exists = check_step3_execution_exists(trade_date)

        if not step3_exec_exists:
            log_entry["reason"] = "Missing step3_execution_control"
            resolution_log.append(log_entry)

            logger.info(f"[REJECTED] {trade_date} | reason=Missing step3_execution_control")
            continue

        log_entry["step3_execution"] = True

        # --------------------------------------
        # STEP 3 stock output check
        # --------------------------------------
        logger.info(f"STEP: Fetching STEP 3 stock count for {trade_date}")
        stock_count = get_step3_stock_count(trade_date)
        log_entry["stock_count"] = stock_count

        if stock_count == 0:
            log_entry["reason"] = "No stocks selected in STEP 3"
            resolution_log.append(log_entry)

            logger.info(f"[REJECTED] {trade_date} | stock_count=0 | reason=No STEP 3 output")
            continue

        # --------------------------------------
        # VALID TRADE DATE FOUND
        # --------------------------------------
        log_entry["status"] = "SELECTED"
        log_entry["reason"] = "STEP 3 produced output"

        resolution_log.append(log_entry)

        logger.info(
            f"[SELECTED] trade_date={trade_date} | "
            f"stock_count={stock_count}"
        )

        logger.info("STEP: Trade Date Resolution completed")

        return trade_date, resolution_log

    # --------------------------------------
    # FINAL DEBUG LOG
    # --------------------------------------
    logger.error("No valid trade_date found after validation")

    logger.error("===== TRADE DATE RESOLUTION DEBUG =====")

    for entry in resolution_log:
        logger.error(
            f"{entry['trade_date']} | "
            f"step3_exec={entry['step3_execution']} | "
            f"stock_count={entry['stock_count']} | "
            f"instruments={entry['instrument_count']} | "
            f"reason={entry['reason']}"
        )

    logger.error("======================================")

    raise Exception("Trade date resolution failed: No valid date found")