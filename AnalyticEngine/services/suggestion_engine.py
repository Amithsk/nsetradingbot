#AnalyticEngine/services/suggestion_engine.py
from AnalyticEngine.utils.logger import get_logger


logger = get_logger(__name__)


def run_suggestion_engine(aggregated_metrics, config):
    """
    Module 5 — Suggestion Engine

    INPUT:
        aggregated_metrics (dict)
        config (dict) -> contains thresholds, rules

    PROCESS:
        - Compare performance metrics against config thresholds
        - Generate suggestions ONLY if conditions are met

    OUTPUT:
        list[dict] (suggestions)
    """

    if not aggregated_metrics:
        logger.warning("No aggregated metrics — skipping suggestion engine")
        return []

    suggestions = []

    try:
        conversion_rate = aggregated_metrics.get("conversion_rate", 0)
        failure_rate = aggregated_metrics.get("failure_rate", 0)
        missed_rate = aggregated_metrics.get("missed_opportunity_rate", 0)

        # --------------------------------------
        # Load thresholds from config
        # --------------------------------------
        conversion_threshold = config.get("conversion_rate_min", 0.4)
        failure_threshold = config.get("failure_rate_max", 0.5)
        missed_threshold = config.get("missed_opportunity_rate_max", 0.3)

        # --------------------------------------
        # Rule 1 — Low conversion rate
        # --------------------------------------
        if conversion_rate < conversion_threshold:

            suggestions.append({
                "rule_name": "conversion_rate",
                "current_value": conversion_rate,
                "suggested_value": conversion_threshold,
                "support_metric": "conversion_rate below threshold",
                "impact": conversion_threshold - conversion_rate,
                "confidence": 0.7,
                "priority": "HIGH" if conversion_rate < 0.3 else "MEDIUM"
            })

        # --------------------------------------
        # Rule 2 — High failure rate
        # --------------------------------------
        if failure_rate > failure_threshold:

            suggestions.append({
                "rule_name": "failure_rate",
                "current_value": failure_rate,
                "suggested_value": failure_threshold,
                "support_metric": "failure_rate above threshold",
                "impact": failure_rate - failure_threshold,
                "confidence": 0.6,
                "priority": "HIGH" if failure_rate > 0.6 else "MEDIUM"
            })

        # --------------------------------------
        # Rule 3 — High missed opportunities
        # --------------------------------------
        if missed_rate > missed_threshold:

            suggestions.append({
                "rule_name": "missed_opportunity_rate",
                "current_value": missed_rate,
                "suggested_value": missed_threshold,
                "support_metric": "missed opportunities too high",
                "impact": missed_rate - missed_threshold,
                "confidence": 0.65,
                "priority": "HIGH" if missed_rate > 0.5 else "MEDIUM"
            })

        logger.info(f"Suggestions generated: {suggestions}")

        return suggestions

    except Exception as e:
        logger.error(f"Suggestion engine failed: {str(e)}")
        return []