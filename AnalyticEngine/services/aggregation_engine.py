#AnalyticEngine/services/aggregation_service.py
from AnalyticEngine.utils.logger import get_logger


logger = get_logger(__name__)


def run_aggregation(per_day_results):
    """
    Module 4 — Aggregation

    INPUT:
        per_day_results (list[dict])

        Each item:
        {
            "candidate_count": int,
            "selected_count": int,
            "good_selection": int,
            "bad_selection": int,
            "missed_opportunity": int
        }

    PROCESS:
        - Aggregate totals across days
        - Compute:
            conversion_rate
            failure_rate
            missed_opportunity_rate

    OUTPUT:
        dict (aggregated_metrics)
    """

    if not per_day_results:
        logger.warning("No per-day results — skipping aggregation")
        return None

    try:
        total_candidates = 0
        total_selected = 0
        total_success = 0
        total_failure = 0
        total_missed = 0

        # --------------------------------------
        # Aggregate totals
        # --------------------------------------
        for day in per_day_results:
            total_candidates += day.get("candidate_count", 0)
            total_selected += day.get("selected_count", 0)
            total_success += day.get("good_selection", 0)
            total_failure += day.get("bad_selection", 0)
            total_missed += day.get("missed_opportunity", 0)

        # --------------------------------------
        # Derived metrics
        # --------------------------------------
        conversion_rate = (
            total_success / total_selected
            if total_selected > 0 else 0
        )

        failure_rate = (
            total_failure / total_selected
            if total_selected > 0 else 0
        )

        missed_opportunity_rate = (
            total_missed / total_candidates
            if total_candidates > 0 else 0
        )

        result = {
            "total_candidates": total_candidates,
            "total_selected": total_selected,
            "total_success": total_success,
            "total_failure": total_failure,
            "total_missed_opportunities": total_missed,
            "conversion_rate": conversion_rate,
            "failure_rate": failure_rate,
            "missed_opportunity_rate": missed_opportunity_rate
        }

        logger.info(f"Aggregation result: {result}")

        return result

    except Exception as e:
        logger.error(f"Aggregation failed: {str(e)}")
        return None