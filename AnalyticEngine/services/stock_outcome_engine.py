#AnalyticEngine/services/stock_outcome.py
def run_stock_outcome_engine(stock_data, direction, logger):
    """
    Module 2 — Stock Outcome Engine
    """

    if not stock_data:
        logger.warning("Stock data missing — skipping outcome calculation")
        return None

    try:
        opens = [row["open"] for row in stock_data]
        highs = [row["high"] for row in stock_data]
        lows = [row["low"] for row in stock_data]
        closes = [row["close"] for row in stock_data]

        day_open = opens[0]

        max_high = max(highs)
        min_low = min(lows)

        # --------------------------------------
        # Move calculations (%)
        # --------------------------------------
        max_up_move_pct = ((max_high - day_open) / day_open) * 100
        max_down_move_pct = ((min_low - day_open) / day_open) * 100
        range_pct = ((max_high - min_low) / day_open) * 100

        # --------------------------------------
        # Outcome classification
        # --------------------------------------
        outcome = "CHOP"

        if direction == "LONG":
            if max_up_move_pct >= 1.5:
                outcome = "SUCCESS"
            elif max_down_move_pct <= -1.0:
                outcome = "FAILURE"

        elif direction == "SHORT":
            if max_down_move_pct <= -1.5:
                outcome = "SUCCESS"
            elif max_up_move_pct >= 1.0:
                outcome = "FAILURE"

        result = {
            "max_up_move_pct": max_up_move_pct,
            "max_down_move_pct": max_down_move_pct,
            "range_pct": range_pct,
            "outcome": outcome
        }

        logger.info(f"STEP: Stock Outcome computed | result={result}")

        return result

    except Exception as e:
        logger.error(f"STEP: Stock Outcome failed | error={str(e)}")
        return None