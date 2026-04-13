# AnalyticalEngine/orchestrator/run_analysis.py

from datetime import datetime

from AnalyticalEngine.services.trade_date_resolver import resolve_trade_date
from AnalyticalEngine.services.validation_engine import run_validation
from AnalyticalEngine.services.data_loader import load_data

from AnalyticalEngine.services.nifty_analysis import run_nifty_analysis
from AnalyticalEngine.services.stock_outcome_engine import run_stock_outcome_engine
from AnalyticalEngine.services.conversion_analysis import run_conversion_analysis
from AnalyticalEngine.services.aggregation_engine import run_aggregation_engine
from AnalyticalEngine.services.suggestion_engine import run_suggestion_engine
from AnalyticalEngine.services.summary_engine import run_summary_engine

from AnalyticalEngine.repositories.ml_repo import (
    save_nifty_insights,
    save_stock_insights,
    save_stock_diagnostics,
    save_suggestions,
    save_summary
)

from AnalyticalEngine.repositories.job_repo import (
    create_job,
    update_job_status
)

from AnalyticalEngine.utils.logger import get_logger

logger = get_logger(__name__)


def run_analysis():
    execution_id = None

    try:
        logger.info("Starting Analytical Engine execution")

        # -----------------------------
        # JOB CREATION
        # -----------------------------
        execution_id = create_job()
        logger.info(f"Job created with execution_id={execution_id}")

        # -----------------------------
        # STEP 1 — TRADE DATE RESOLUTION
        # -----------------------------
        trade_date, resolution_log = resolve_trade_date()
        logger.info(f"Resolved trade_date={trade_date}")

        # -----------------------------
        # STEP 2 — VALIDATION
        # -----------------------------
        validation_result = run_validation(trade_date)
        analysis_status = validation_result.get("status")

        logger.info(f"Validation status={analysis_status}")

        if analysis_status == "INVALID":
            update_job_status(
                execution_id=execution_id,
                status="SKIPPED",
                end_time=datetime.utcnow()
            )
            logger.warning("Validation failed. Skipping execution.")
            return

        # -----------------------------
        # STEP 3 — DATA LOADING
        # -----------------------------
        data_bundle = load_data(trade_date)
        logger.info("Data loaded successfully")

        # -----------------------------
        # MODULE 1 — NIFTY ANALYSIS
        # -----------------------------
        nifty_output = None
        if validation_result.get("nifty") != "MISSING":
            nifty_output = run_nifty_analysis(data_bundle)

        # -----------------------------
        # MODULE 2 — STOCK OUTCOME
        # -----------------------------
        stock_outcomes = None
        if validation_result.get("stock") != "MISSING":
            stock_outcomes = run_stock_outcome_engine(data_bundle)

        # -----------------------------
        # MODULE 3 — CONVERSION ANALYSIS
        # -----------------------------
        conversion_output = None
        if stock_outcomes and validation_result.get("step3") != "MISSING":
            conversion_output = run_conversion_analysis(
                data_bundle,
                stock_outcomes
            )

        # -----------------------------
        # MODULE 4 — AGGREGATION
        # -----------------------------
        aggregation_output = run_aggregation_engine(trade_date)

        # -----------------------------
        # MODULE 5 — SUGGESTION ENGINE
        # -----------------------------
        suggestions = run_suggestion_engine(aggregation_output)

        # -----------------------------
        # MODULE 6 — SUMMARY ENGINE
        # -----------------------------
        summary = run_summary_engine(
            nifty_output,
            conversion_output,
            suggestions
        )

        # -----------------------------
        # STORAGE (IDEMPOTENT)
        # -----------------------------
        if nifty_output:
            save_nifty_insights(trade_date, nifty_output, analysis_status)

        if conversion_output:
            save_stock_insights(trade_date, conversion_output, analysis_status)

        if stock_outcomes:
            save_stock_diagnostics(trade_date, stock_outcomes)

        if suggestions:
            save_suggestions(trade_date, suggestions)

        if summary:
            save_summary(trade_date, summary, analysis_status)

        # -----------------------------
        # JOB COMPLETION
        # -----------------------------
        final_status = (
            "PARTIAL" if analysis_status == "PARTIAL" else "COMPLETED"
        )

        update_job_status(
            execution_id=execution_id,
            status=final_status,
            end_time=datetime.utcnow()
        )

        logger.info(f"Execution completed with status={final_status}")

    except Exception as e:
        logger.error(f"Execution failed: {str(e)}")

        if execution_id:
            update_job_status(
                execution_id=execution_id,
                status="FAILED",
                end_time=datetime.utcnow()
            )

        raise