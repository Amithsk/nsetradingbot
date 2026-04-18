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
logger = setup_execution_logger()




def run_analysis(config):
    execution_id = None
    trade_date = None
    logger = logging.getLogger("AnalyticEngine")

    try:
        # --------------------------------------
        # 1. Trade Date Resolution
        # --------------------------------------
        trade_date, resolution_log = resolve_trade_date()

        print(f"Resolved Trade Date: {trade_date}")

        # --------------------------------------
        # 2. Prevent duplicate RUNNING job
        # --------------------------------------
        running_job = get_running_job(trade_date)

        if running_job:
            print(f"Existing RUNNING job found for {trade_date}: {running_job['execution_id']}")
           
            return

        # --------------------------------------
        # 3. Job Creation
        # --------------------------------------
        execution_id = create_job(trade_date)
        logger = setup_execution_logger(execution_id)
        update_job_status(execution_id, "RUNNING")

        print(f"Job started with Execution ID: {execution_id}")
        logger.info(f"Job started: {execution_id}")

        # --------------------------------------
        # 4. Validation
        # --------------------------------------
        validation_status, validation_log = run_validation(trade_date)

        if validation_status == "SKIPPED":
            logger.warning("Validation failed — skipping execution")
            complete_job(execution_id, "SKIPPED")
            return

        # --------------------------------------
        # 5. Idempotency Cleanup
        # --------------------------------------
        # NOTE: Assuming this function exists elsewhere
        # cleanup_existing_outputs(trade_date)

        # --------------------------------------
        # 6. Data Loading
        # --------------------------------------
        data = load_data(trade_date)

        if not data:
            logger.error("Data loading failed")
            complete_job(execution_id, "FAILED")
            return

        # --------------------------------------
        # 7. Module 1 — NIFTY
        # --------------------------------------
        nifty_metrics = run_nifty_analysis(
            data["nifty_data"],
            data["step1_data"],
            data["step2_data"]
        )

        # --------------------------------------
        # 8. Module 2 — Stock Outcome
        # --------------------------------------
        stock_outcomes = {}

        for symbol, stock_data in data["stock_data"].items():
            outcome = run_stock_outcome_engine(stock_data, "LONG")
            stock_outcomes[symbol] = outcome

        # --------------------------------------
        # 9. Module 3 — Conversion
        # --------------------------------------
        conversion_result = run_conversion_analysis(
            data["step3_data"],
            stock_outcomes
        )

        summary_metrics = conversion_result["summary"]
        diagnostics = conversion_result["diagnostics"]

        # --------------------------------------
        # 10. Module 4 — Aggregation
        # --------------------------------------
        aggregated_metrics = run_aggregation([summary_metrics])

        # --------------------------------------
        # 11. Module 5 — Suggestions
        # --------------------------------------
        suggestions = run_suggestion_engine(aggregated_metrics, config)

        # --------------------------------------
        # 12. Module 6 — Summary
        # --------------------------------------
        summary_text = run_summary_engine(aggregated_metrics, suggestions, config)

        # --------------------------------------
        # 13. Storage
        # --------------------------------------
        rule_version = config.get("rule_config_version", "v1")

        insert_nifty_insights(trade_date, nifty_metrics, validation_status, rule_version)
        insert_stock_insights(trade_date, aggregated_metrics, validation_status, rule_version)
        insert_stock_diagnostics(trade_date, diagnostics)
        insert_suggestions(trade_date, suggestions)
        insert_summary(trade_date, summary_text, validation_status, rule_version)

        # --------------------------------------
        # 14. Job Completion
        # --------------------------------------
        final_status = "COMPLETED" if validation_status == "COMPLETE" else "PARTIAL"

        complete_job(execution_id, final_status)

        logger.info(f"Job completed: {execution_id}")

    except Exception as e:
        logger.error(f"Execution failed: {str(e)}")

        if execution_id:
            complete_job(execution_id, "FAILED")

if __name__ == "__main__":
    print(" Starting Analytical Engine...")

    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "rule_config_v1.json")    )

    print(f"Loading config from: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    run_analysis(config)

    print(" Execution finished")
