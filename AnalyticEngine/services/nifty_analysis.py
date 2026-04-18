#AnalyticEngine/services/nifty_analysis.py
def run_nifty_analysis(nifty_data, step1_data, step2_data, logger):
    """
    Module 1 — NIFTY Analysis
    """

    logger.info("STEP: NIFTY Analysis started")

    if not nifty_data:
        logger.warning("NIFTY data missing — skipping analysis")
        return None

    try:
        logger.info(f"NIFTY records received: {len(nifty_data)}")

        opens = [row["open"] for row in nifty_data]
        highs = [row["high"] for row in nifty_data]
        lows = [row["low"] for row in nifty_data]
        closes = [row["close"] for row in nifty_data]

        day_open = opens[0]
        day_close = closes[-1]

        total_range = max(highs) - min(lows)
        net_move = day_close - day_open

        trend_strength = abs(net_move) / total_range if total_range != 0 else 0

        # --------------------------------------
        # Pullback Depth
        # --------------------------------------
        if net_move >= 0:
            peak = max(highs)
            pullback = peak - day_close
        else:
            trough = min(lows)
            pullback = day_close - trough

        pullback_depth = pullback / total_range if total_range != 0 else 0

        # --------------------------------------
        # VWAP Approximation
        # --------------------------------------
        cumulative_price = 0
        cumulative_count = 0
        vwap_values = []

        for row in nifty_data:
            typical_price = (row["high"] + row["low"] + row["close"]) / 3
            cumulative_price += typical_price
            cumulative_count += 1
            vwap = cumulative_price / cumulative_count
            vwap_values.append(vwap)

        # --------------------------------------
        # VWAP Cross Count
        # --------------------------------------
        vwap_cross_count = 0

        for i in range(1, len(closes)):
            prev_diff = closes[i - 1] - vwap_values[i - 1]
            curr_diff = closes[i] - vwap_values[i]

            if prev_diff * curr_diff < 0:
                vwap_cross_count += 1

        # --------------------------------------
        # VWAP Hold %
        # --------------------------------------
        direction = 1 if net_move >= 0 else -1

        hold_count = 0

        for i in range(len(closes)):
            diff = closes[i] - vwap_values[i]

            if (direction == 1 and diff > 0) or (direction == -1 and diff < 0):
                hold_count += 1

        vwap_hold_percentage = hold_count / len(closes) if closes else 0

        result = {
            "total_range": total_range,
            "net_move": net_move,
            "trend_strength": trend_strength,
            "pullback_depth": pullback_depth,
            "vwap_cross_count": vwap_cross_count,
            "vwap_hold_percentage": vwap_hold_percentage
        }

        logger.info(f"STEP: NIFTY Analysis completed | result={result}")

        return result

    except Exception as e:
        logger.error(f"STEP: NIFTY Analysis failed | error={str(e)}")
        return None