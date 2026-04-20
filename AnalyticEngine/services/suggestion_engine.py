#AnalyticEngine/services/suggestion_engine.py
def run_suggestion_engine(aggregated_metrics, config, logger):
    """
    Module 5 — Suggestion Engine
    """

    logger.info("STEP: Suggestion Engine started")

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

        logger.info(
            f"Thresholds | conversion_min={conversion_threshold} | "
            f"failure_max={failure_threshold} | missed_max={missed_threshold}"
        )

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

        logger.info(f"STEP: Suggestion Engine completed | suggestions_count={len(suggestions)}")

        return suggestions

    except Exception as e:
        logger.error(f"STEP: Suggestion Engine failed | error={str(e)}")
        return []