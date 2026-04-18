from datetime import datetime

import logging
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from AnalyticEngine.services.trade_date_resolver import resolve_trade_date
from AnalyticEngine.services.validation_engine import run_validation
from AnalyticEngine.services.data_loader import load_data
from AnalyticEngine.services.nifty_analysis import run_nifty_analysis
from AnalyticEngine.services.stock_outcome_engine import run_stock_outcome_engine
from AnalyticEngine.services.conversion_analysis import run_conversion_analysis
from AnalyticEngine.services.aggregation_engine import run_aggregation
from AnalyticEngine.services.suggestion_engine import run_suggestion_engine
from AnalyticEngine.services.summary_engine import run_summary_engine

from AnalyticEngine.repositories.ml_repo import (
    insert_nifty_insights,
    insert_stock_insights,
    insert_stock_diagnostics,
    insert_suggestions,
    insert_summary
)

from AnalyticEngine.repositories.job_repo import (
    create_job,
    update_job_status,
    complete_job,
    get_running_job
)

from AnalyticEngine.utils.logger import setup_execution_logger


def run_analysis(config):
    execution_id = None
    trade_date = None

    # Pre-execution logger
    logger = logging.getLogger("AnalyticEngine")

    logger.info("===== ANALYTICAL ENGINE STARTED =====")

    try:
        # --------------------------------------
        # 1. Trade Date Resolution
        # --------------------------------------
        logger.info("STEP 1: Trade Date Resolution START")

        trade_date, resolution_log = resolve_trade_date()

        logger.info(f"STEP 1 COMPLETE | trade_date={trade_date}")

        # --------------------------------------
        # 2. Prevent duplicate RUNNING job
        # --------------------------------------
        logger.info("STEP 2: Checking existing RUNNING job")

        try:
            running_job = get_running_job(trade_date)
        except Exception as e:
            logger.error(f"FAILED at job check (get_running_job): {str(e)}")
            raise

        if running_job:
            logger.warning(f"RUNNING job already exists for {trade_date}")
            return

        logger.info("STEP 2 COMPLETE | No running job found")

        # --------------------------------------
        # 3. Job Creation
        # --------------------------------------
        logger.info("STEP 3: Creating job")

        try:
            execution_id = create_job(trade_date)
        except Exception as e:
            logger.error(f"FAILED at job creation: {str(e)}")
            raise

        logger = setup_execution_logger(execution_id)

        update_job_status(execution_id, "RUNNING")

        logger.info(f"STEP 3 COMPLETE | execution_id={execution_id}")

        # --------------------------------------
        # 4. Validation
        # --------------------------------------
        logger.info("STEP 4: Validation START")

        validation_status, validation_log = run_validation(trade_date)

        logger.info(f"STEP 4 COMPLETE | status={validation_status}")

        if validation_status == "SKIPPED":
            logger.warning("Validation SKIPPED — stopping execution")
            complete_job(execution_id, "SKIPPED")
            return

        # --------------------------------------
        # 5. Data Loading
        # --------------------------------------
        logger.info("STEP 5: Data Loading START")

        data = load_data(trade_date)

        if not data:
            logger.error("STEP 5 FAILED | Data loading returned empty")
            complete_job(execution_id, "FAILED")
            return

        logger.info("STEP 5 COMPLETE")

        # --------------------------------------
        # 6. NIFTY Analysis
        # --------------------------------------
        logger.info("STEP 6: NIFTY Analysis")

        nifty_metrics = run_nifty_analysis(
            data["nifty_data"],
            data["step1_data"],
            data["step2_data"]
        )

        logger.info("STEP 6 COMPLETE")

        # --------------------------------------
        # 7. Stock Outcome
        # --------------------------------------
        logger.info("STEP 7: Stock Outcome Analysis")

        stock_outcomes = {}

        for symbol, stock_data in data["stock_data"].items():
            outcome = run_stock_outcome_engine(stock_data, "LONG")
            stock_outcomes[symbol] = outcome

        logger.info(f"STEP 7 COMPLETE | processed={len(stock_outcomes)} stocks")

        # --------------------------------------
        # 8. Conversion Analysis
        # --------------------------------------
        logger.info("STEP 8: Conversion Analysis")

        conversion_result = run_conversion_analysis(
            data["step3_data"],
            stock_outcomes
        )

        summary_metrics = conversion_result["summary"]
        diagnostics = conversion_result["diagnostics"]

        logger.info("STEP 8 COMPLETE")

        # --------------------------------------
        # 9. Aggregation
        # --------------------------------------
        logger.info("STEP 9: Aggregation")

        aggregated_metrics = run_aggregation([summary_metrics])

        logger.info("STEP 9 COMPLETE")

        # --------------------------------------
        # 10. Suggestions
        # --------------------------------------
        logger.info("STEP 10: Suggestion Engine")

        suggestions = run_suggestion_engine(aggregated_metrics, config)

        logger.info(f"STEP 10 COMPLETE | suggestions={len(suggestions)}")

        # --------------------------------------
        # 11. Summary
        # --------------------------------------
        logger.info("STEP 11: Summary Generation")

        summary_text = run_summary_engine(aggregated_metrics, suggestions, config)

        logger.info("STEP 11 COMPLETE")

        # --------------------------------------
        # 12. Storage
        # --------------------------------------
        logger.info("STEP 12: Persisting outputs")

        rule_version = config.get("rule_config_version", "v1")

        insert_nifty_insights(trade_date, nifty_metrics, validation_status, rule_version)
        insert_stock_insights(trade_date, aggregated_metrics, validation_status, rule_version)
        insert_stock_diagnostics(trade_date, diagnostics)
        insert_suggestions(trade_date, suggestions)
        insert_summary(trade_date, summary_text, validation_status, rule_version)

        logger.info("STEP 12 COMPLETE")

        # --------------------------------------
        # 13. Job Completion
        # --------------------------------------
        logger.info("STEP 13: Completing job")

        final_status = "COMPLETED" if validation_status == "COMPLETE" else "PARTIAL"

        complete_job(execution_id, final_status)

        logger.info(f"===== JOB COMPLETED | execution_id={execution_id} | status={final_status} =====")

    except Exception as e:
        logger.error(f"===== EXECUTION FAILED =====")
        logger.error(f"ERROR: {str(e)}")

        if execution_id:
            complete_job(execution_id, "FAILED")

        raise


if __name__ == "__main__":
    print(" Starting Analytical Engine...")

    config_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "config", "rule_config_v1.json")
    )

    print(f"Loading config from: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    run_analysis(config)

    print(" Execution finished")