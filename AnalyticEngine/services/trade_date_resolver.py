# AnalyticalEngine/services/trade_date_resolver.py

from AnalyticEngine.repositories.step_repo import (
    get_available_trade_dates,
    check_step3_execution_exists,
    get_step3_stock_count
)

from AnalyticEngine.repositories.stock_repo import (
    get_instruments_count
)

from AnalyticEngine.utils.logger import get_logger

logger = get_logger(__name__)


def resolve_trade_date():
    resolution_log = []

    trade_dates = get_available_trade_dates()

    if not trade_dates:
        logger.error("No trade_dates found in step3_execution_control")
        raise Exception("Trade date resolution failed: No available dates")

    total_instruments = get_instruments_count()
    logger.info(f"Total instruments in universe: {total_instruments}")

    for trade_date in trade_dates:

        log_entry = {
            "trade_date": trade_date,
            "step3_execution": False,
            "stock_count": 0,
            "instrument_count": total_instruments,
            "status": "REJECTED",
            "reason": None
        }

        step3_exec_exists = check_step3_execution_exists(trade_date)

        if not step3_exec_exists:
            log_entry["reason"] = "Missing step3_execution_control"
            resolution_log.append(log_entry)

            logger.info(f"[REJECTED] {trade_date} | reason: {log_entry['reason']}")
            continue

        log_entry["step3_execution"] = True

        stock_count = get_step3_stock_count(trade_date)
        log_entry["stock_count"] = stock_count

        if stock_count == 0:
            log_entry["reason"] = "No records in step3_stock_selection"
            resolution_log.append(log_entry)

            logger.info(f"[REJECTED] {trade_date} | stock_count=0")
            continue

        if stock_count < total_instruments:
            log_entry["reason"] = "Incomplete stock coverage"
            resolution_log.append(log_entry)

            logger.info(
                f"[REJECTED] {trade_date} | stock_count={stock_count} < instruments={total_instruments}"
            )
            continue

        # ✅ SUCCESS
        log_entry["status"] = "SELECTED"
        log_entry["reason"] = "All validation checks passed"

        resolution_log.append(log_entry)

        logger.info(f"[SELECTED] trade_date={trade_date}")

        return trade_date, resolution_log

    # --------------------------------------
    # FINAL DEBUG LOG (CRITICAL)
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