#AnalyticEngine/services/conversion_analysis.py
from AnalyticEngine.utils.logger import get_logger


logger = get_logger(__name__)


def run_conversion_analysis(step3_candidates, stock_outcomes):
    """
    Module 3 — Conversion Analysis

    INPUT:
        step3_candidates (list[dict])
        stock_outcomes (dict) -> {symbol: outcome_data}

    PROCESS:
        - candidate_count
        - selected_count
        - conversion_rate
        - classification per stock:
            GOOD_SELECTION
            BAD_SELECTION
            MISSED_OPPORTUNITY
            CORRECT_REJECTION

    OUTPUT:
        dict:
            {
                "summary": {...},
                "diagnostics": list[dict]
            }
    """

    if not step3_candidates:
        logger.warning("STEP 3 candidates missing — skipping conversion analysis")
        return None

    try:
        diagnostics = []

        candidate_count = len(step3_candidates)
        selected_count = 0

        good_selection = 0
        bad_selection = 0
        missed_opportunity = 0
        correct_rejection = 0

        for stock in step3_candidates:

            symbol = stock.get("symbol")
            selected = stock.get("selected", False)

            outcome_data = stock_outcomes.get(symbol, {})
            outcome = outcome_data.get("outcome")

            classification = None

            # --------------------------------------
            # Classification logic
            # --------------------------------------
            if selected:
                selected_count += 1

                if outcome == "SUCCESS":
                    classification = "GOOD_SELECTION"
                    good_selection += 1
                elif outcome == "FAILURE":
                    classification = "BAD_SELECTION"
                    bad_selection += 1
                else:
                    classification = "CHOP"

            else:
                if outcome == "SUCCESS":
                    classification = "MISSED_OPPORTUNITY"
                    missed_opportunity += 1
                else:
                    classification = "CORRECT_REJECTION"
                    correct_rejection += 1

            diagnostics.append({
                "symbol": symbol,
                "selected": selected,
                "outcome": outcome,
                "classification": classification
            })

        # --------------------------------------
        # Summary metrics
        # --------------------------------------
        conversion_rate = (
            good_selection / selected_count
            if selected_count > 0 else 0
        )

        summary = {
            "candidate_count": candidate_count,
            "selected_count": selected_count,
            "good_selection": good_selection,
            "bad_selection": bad_selection,
            "missed_opportunity": missed_opportunity,
            "correct_rejection": correct_rejection,
            "conversion_rate": conversion_rate
        }

        result = {
            "summary": summary,
            "diagnostics": diagnostics
        }

        logger.info(f"Conversion analysis result: {summary}")

        return result

    except Exception as e:
        logger.error(f"Conversion analysis failed: {str(e)}")
        return None