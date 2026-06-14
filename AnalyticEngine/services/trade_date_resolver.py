#AnalyticEngine/services/trade_date_resolver.py
from AnalyticEngine.repositories.step_repo import (
    get_available_trade_dates,
    check_step3_execution_exists,
    get_step3_stock_count
)

from AnalyticEngine.repositories.stock_repo import (
    get_instruments_count
)

from AnalyticEngine.repositories.job_repo import (
    get_completed_trade_dates
)


def resolve_trade_date(logger):
    """
    Trade Date Resolution

    Purpose:
        Identify the next trade_date that requires analytical processing.

    Selection Rules:
        1. Fetch all available trade_dates from step3_execution_control
        2. Remove trade_dates already COMPLETED in ml_job_tracker
        3. Sort remaining dates oldest first
        4. Select first valid trade_date

    Notes:
        - stock_count is collected for diagnostics only
        - stock_count = 0 does NOT reject the trade_date
        - STEP3 execution record must exist
        - Actual validation happens later in Validation Engine
    """

    logger.info("STEP: Trade Date Resolution started")

    resolution_log = []

    # ---------------------------------------------------------
    # Fetch all available trade_dates
    # ---------------------------------------------------------
    logger.info("STEP: Fetching available trade_dates")

    trade_dates = get_available_trade_dates()

    if not trade_dates:
        logger.error("No trade_dates found in step3_execution_control")
        raise Exception("Trade date resolution failed: No available dates")

    logger.info(f"STEP: Found {len(trade_dates)} trade_dates")

    # ---------------------------------------------------------
    # Fetch already completed trade_dates
    #
    # These dates have already been successfully processed
    # by the Analytical Engine and should not be selected
    # again.
    # ---------------------------------------------------------
    logger.info("STEP: Fetching completed trade_dates")

    completed_dates = get_completed_trade_dates()

    logger.info(
        f"STEP: Found {len(completed_dates)} completed trade_dates"
    )

    # ---------------------------------------------------------
    # Remove already processed dates
    # ---------------------------------------------------------
    pending_dates = [
        trade_date
        for trade_date in trade_dates
        if trade_date not in completed_dates
    ]

    logger.info(
        f"STEP: Found {len(pending_dates)} unprocessed trade_dates"
    )

    if not pending_dates:
        logger.info("No unprocessed trade_dates available")
        raise Exception(
            "Trade date resolution failed: No unprocessed trade_dates"
        )

    # ---------------------------------------------------------
    # Process oldest unprocessed date first.
    #
    # step3_execution_control returns DESC order.
    # We intentionally re-sort ASC so that historical
    # backlog is cleared before newer dates.
    # ---------------------------------------------------------
    pending_dates = sorted(pending_dates)

    # ---------------------------------------------------------
    # Fetch instrument count for diagnostics
    # ---------------------------------------------------------
    logger.info("STEP: Fetching instrument universe size")

    total_instruments = get_instruments_count()

    logger.info(
        f"Total instruments in universe: {total_instruments}"
    )

    # ---------------------------------------------------------
    # Iterate pending dates
    # ---------------------------------------------------------
    for trade_date in pending_dates:

        logger.info(f"CHECKING trade_date={trade_date}")

        log_entry = {
            "trade_date": trade_date,
            "step3_execution": False,
            "stock_count": 0,
            "instrument_count": total_instruments,
            "status": "REJECTED",
            "reason": None
        }

        # -----------------------------------------------------
        # Verify step3_execution_control exists
        # -----------------------------------------------------
        logger.info(
            f"STEP: Checking step3_execution_control for {trade_date}"
        )

        step3_exec_exists = check_step3_execution_exists(trade_date)

        if not step3_exec_exists:

            log_entry["reason"] = (
                "Missing step3_execution_control"
            )

            resolution_log.append(log_entry)

            logger.info(
                f"[REJECTED] {trade_date} | "
                f"reason=Missing step3_execution_control"
            )

            continue

        log_entry["step3_execution"] = True

        # -----------------------------------------------------
        # Fetch STEP3 stock count
        #
        # IMPORTANT:
        # stock_count is informational only.
        #
        # A trade_date remains valid even when
        # STEP3 selected zero stocks.
        #
        # NIFTY analysis and learning analysis
        # must still be allowed to run.
        # -----------------------------------------------------
        logger.info(
            f"STEP: Fetching STEP 3 stock count for {trade_date}"
        )

        stock_count = get_step3_stock_count(trade_date)

        log_entry["stock_count"] = stock_count

        # -----------------------------------------------------
        # Valid unprocessed trade_date found
        # -----------------------------------------------------
        log_entry["status"] = "SELECTED"

        if stock_count == 0:
            log_entry["reason"] = (
                "Valid trade_date with no STEP3 selections"
            )
        else:
            log_entry["reason"] = (
                "Valid trade_date with STEP3 output"
            )

        resolution_log.append(log_entry)

        logger.info(
            f"[SELECTED] trade_date={trade_date} | "
            f"stock_count={stock_count}"
        )

        logger.info("STEP: Trade Date Resolution completed")

        return trade_date, resolution_log

    # ---------------------------------------------------------
    # Final diagnostic output
    # ---------------------------------------------------------
    logger.error("No valid trade_date found after validation")

    logger.error(
        "===== TRADE DATE RESOLUTION DEBUG ====="
    )

    for entry in resolution_log:

        logger.error(
            f"{entry['trade_date']} | "
            f"step3_exec={entry['step3_execution']} | "
            f"stock_count={entry['stock_count']} | "
            f"instruments={entry['instrument_count']} | "
            f"reason={entry['reason']}"
        )

    logger.error(
        "======================================"
    )

    raise Exception(
        "Trade date resolution failed: No valid date found"
    )