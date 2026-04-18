#AnalyticEngine/services/summary_engine.py
def run_summary_engine(aggregated_metrics, suggestions, config, logger):
    """
    Module 6 — Summary Generation
    """

    logger.info("STEP: Summary Generation started")

    if not aggregated_metrics:
        logger.warning("No aggregated metrics — skipping summary generation")
        return ""

    try:
        summary_parts = []

        # --------------------------------------
        # Load config
        # --------------------------------------
        thresholds = config.get("metric_thresholds", {})
        label_map = config.get("label_mappings", {})
        templates = config.get("templates", {})
        summary_rules = config.get("summary_rules", {})

        conversion_rate = aggregated_metrics.get("conversion_rate", 0)
        failure_rate = aggregated_metrics.get("failure_rate", 0)
        missed_rate = aggregated_metrics.get("missed_opportunity_rate", 0)

        logger.info(
            f"Metrics | conversion={conversion_rate} | "
            f"failure={failure_rate} | missed={missed_rate}"
        )

        # --------------------------------------
        # Metric → Label
        # --------------------------------------
        def get_label(metric_name, value):
            metric_rules = thresholds.get(metric_name, [])

            for rule in metric_rules:
                rule_type = rule.get("type")
                threshold = rule.get("value")

                if rule_type == "lt" and value < threshold:
                    return label_map.get(rule["label"], rule["label"])
                elif rule_type == "lte" and value <= threshold:
                    return label_map.get(rule["label"], rule["label"])
                elif rule_type == "gt" and value > threshold:
                    return label_map.get(rule["label"], rule["label"])
                elif rule_type == "gte" and value >= threshold:
                    return label_map.get(rule["label"], rule["label"])

            return "UNKNOWN"

        # --------------------------------------
        # Generate labels
        # --------------------------------------
        conversion_label = get_label("conversion_rate", conversion_rate)
        failure_label = get_label("failure_rate", failure_rate)
        missed_label = get_label("missed_opportunity_rate", missed_rate)

        logger.info(
            f"Labels | conversion={conversion_label} | "
            f"failure={failure_label} | missed={missed_label}"
        )

        # --------------------------------------
        # Label → Template
        # --------------------------------------
        if conversion_label in templates:
            summary_parts.append(templates[conversion_label])

        if failure_label in templates:
            summary_parts.append(templates[failure_label])

        if missed_label in templates:
            summary_parts.append(templates[missed_label])

        # --------------------------------------
        # Suggestion summary
        # --------------------------------------
        if suggestions:
            summary_parts.append(f"{len(suggestions)} improvement opportunities identified.")

        # --------------------------------------
        # Apply summary rules
        # --------------------------------------
        max_lines = summary_rules.get("max_lines", 5)
        summary_parts = summary_parts[:max_lines]

        summary_text = " ".join(summary_parts)

        logger.info(f"STEP: Summary Generation completed | summary={summary_text}")

        return summary_text

    except Exception as e:
        logger.error(f"STEP: Summary Generation failed | error={str(e)}")
        return ""