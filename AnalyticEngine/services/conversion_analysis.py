#AnalyticEngine/services/conversion_analysis.py
def run_conversion_analysis(step3_candidates, stock_outcomes, logger):
    """
    Module 3 — Conversion Analysis
    """

    logger.info("STEP: Conversion Analysis started")

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

        # --------------------------------------
        # 🔍 TRACK MISSING OUTCOMES (ENHANCED)
        # --------------------------------------
        missing_outcomes = 0
        missing_symbols = []

        for stock in step3_candidates:

            symbol = stock.get("symbol")
            selected = stock.get("selected", False)

            outcome_data = stock_outcomes.get(symbol, None)
            outcome = outcome_data.get("outcome") if outcome_data else None

            # --------------------------------------
            # TRACK MISSING OUTCOME SYMBOLS
            # --------------------------------------
            if outcome is None:
                missing_outcomes += 1
                missing_symbols.append(symbol)

            classification = None

            # --------------------------------------
            # Classification logic (UNCHANGED)
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

        # --------------------------------------
        # CLEAN + ACTIONABLE LOG
        # --------------------------------------
        logger.info(
            f"STEP: Conversion Analysis completed | "
            f"candidates={candidate_count} | selected={selected_count} | "
            f"success={good_selection} | failure={bad_selection} | "
            f"missed={missed_opportunity} | "
            f"missing_outcomes={missing_outcomes} | "
            f"missing_symbols={missing_symbols}"
        )

        return result

    except Exception as e:
        logger.error(f"STEP: Conversion Analysis failed | error={str(e)}")
        return None