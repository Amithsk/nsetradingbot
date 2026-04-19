# AnalyticEngine/repositories/ml_repo.py
from AnalyticEngine.utils.db_connection import get_db_connection
from AnalyticEngine.utils.db_schemas import ML_SCHEMA,NIFTY_SCHEMA
from datetime import datetime
from sqlalchemy import text


def get_nifty_data(trade_date):
    engine = get_db_connection()

    query = f"""
        SELECT Date, open, high, low, close
        FROM {NIFTY_SCHEMA}.nifty_prices
        WHERE DATE(Date) = :trade_date
        ORDER BY Date ASC
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        rows = result.fetchall()

    return [
        {
            "datetime": row[0],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4]
        }
        for row in rows
    ]


def get_stock_data(trade_date):
    engine = get_db_connection()

    query = """
        SELECT symbol, open, high, low, close
        FROM intraday_bhavcopy
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        rows = result.fetchall()

    stock_map = {}

    for row in rows:
        symbol = row[0]

        if symbol not in stock_map:
            stock_map[symbol] = []

        stock_map[symbol].append({
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4]
        })

    return stock_map


def get_step1_data(trade_date):
    engine = get_db_connection()

    query = """
        SELECT gap_pct, gap_class,prior_structure_state,final_market_context
        FROM step1_market_context
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    if not row:
        return None

    return {
        "gap_pct": row[0],
        "gap_class": row[1],
        "prior_structure_state": row[2],
        "final_market_context": row[3]
    }


def get_step2_data(trade_date):
    engine = get_db_connection()

    query = """
        SELECT trade_permission, IR_ratio, volatility_state, VWAP_state, range_hold_status
        FROM step2_market_open_behavior
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    if not row:
        return None

    return {
        "trade_permission": row[0],
        "IR_ratio": row[1],
        "volatility_state": row[2],
        "VWAP_state": row[3],
        "range_hold_status": row[4]
    }


def get_step3_data(trade_date):
    engine = get_db_connection()

    query = """
        SELECT 
            symbol,
            tradable AS selected
        FROM intradaytrading.step3_stock_selection
        WHERE trade_date = :trade_date
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"trade_date": trade_date})
        row = result.fetchone()

    if not row:
        return []

    return row[0]


# --------------------------------------
# ML INSERTS (UPDATED WITH SCHEMA)
# --------------------------------------

def insert_nifty_insights(trade_date, metrics, analysis_status, rule_config_version):
    engine = get_db_connection()

    query = f"""
        INSERT INTO {ML_SCHEMA}.ml_nifty_insights (
            trade_date,
            total_range,
            net_move,
            trend_strength,
            pullback_depth,
            vwap_cross_count,
            vwap_hold_percentage,
            analysis_status,
            rule_config_version,
            created_at
        )
        VALUES (:trade_date, :total_range, :net_move, :trend_strength, :pullback_depth,
                :vwap_cross_count, :vwap_hold_percentage, :analysis_status,
                :rule_config_version, :created_at)
    """

    with engine.begin() as conn:
        conn.execute(
            text(query),
            {
                "trade_date": trade_date,
                "total_range": metrics.get("total_range"),
                "net_move": metrics.get("net_move"),
                "trend_strength": metrics.get("trend_strength"),
                "pullback_depth": metrics.get("pullback_depth"),
                "vwap_cross_count": metrics.get("vwap_cross_count"),
                "vwap_hold_percentage": metrics.get("vwap_hold_percentage"),
                "analysis_status": analysis_status,
                "rule_config_version": rule_config_version,
                "created_at": datetime.utcnow()
            }
        )


def insert_stock_insights(trade_date, aggregated_metrics, analysis_status, rule_config_version):
    engine = get_db_connection()

    query = f"""
        INSERT INTO {ML_SCHEMA}.ml_stock_insights (
            trade_date,
            candidate_count,
            selected_count,
            total_success,
            total_failure,
            total_missed_opportunities,
            analysis_status,
            rule_config_version,
            created_at
        )
        VALUES (:trade_date, :candidate_count, :selected_count, :total_success,
                :total_failure, :total_missed_opportunities, :analysis_status,
                :rule_config_version, :created_at)
    """

    with engine.begin() as conn:
        conn.execute(
            text(query),
            {
                "trade_date": trade_date,
                "candidate_count": aggregated_metrics.get("total_candidates"),
                "selected_count": aggregated_metrics.get("total_selected"),
                "total_success": aggregated_metrics.get("total_success"),
                "total_failure": aggregated_metrics.get("total_failure"),
                "total_missed_opportunities": aggregated_metrics.get("total_missed_opportunities"),
                "analysis_status": analysis_status,
                "rule_config_version": rule_config_version,
                "created_at": datetime.utcnow()
            }
        )


def insert_stock_diagnostics(trade_date, diagnostics):
    engine = get_db_connection()

    query = f"""
        INSERT INTO {ML_SCHEMA}.ml_stock_diagnostics (
            trade_date,
            symbol,
            selected,
            outcome,
            classification
        )
        VALUES (:trade_date, :symbol, :selected, :outcome, :classification)
    """

    with engine.begin() as conn:
        for row in diagnostics:
            conn.execute(
                text(query),
                {
                    "trade_date": trade_date,
                    "symbol": row.get("symbol"),
                    "selected": row.get("selected"),
                    "outcome": row.get("outcome"),
                    "classification": row.get("classification")
                }
            )


def insert_suggestions(trade_date, suggestions):
    engine = get_db_connection()

    query = f"""
        INSERT INTO {ML_SCHEMA}.ml_suggestions (
            trade_date,
            rule_name,
            current_value,
            suggested_value,
            support_metric,
            impact,
            confidence,
            priority,
            created_at
        )
        VALUES (:trade_date, :rule_name, :current_value, :suggested_value,
                :support_metric, :impact, :confidence, :priority, :created_at)
    """

    with engine.begin() as conn:
        for s in suggestions:
            conn.execute(
                text(query),
                {
                    "trade_date": trade_date,
                    "rule_name": s.get("rule_name"),
                    "current_value": s.get("current_value"),
                    "suggested_value": s.get("suggested_value"),
                    "support_metric": s.get("support_metric"),
                    "impact": s.get("impact"),
                    "confidence": s.get("confidence"),
                    "priority": s.get("priority"),
                    "created_at": datetime.utcnow()
                }
            )


def insert_summary(trade_date, summary_text, analysis_status, rule_config_version):
    engine = get_db_connection()

    query = f"""
        INSERT INTO {ML_SCHEMA}.ml_summary (
            trade_date,
            summary_text,
            analysis_status,
            rule_config_version,
            created_at
        )
        VALUES (:trade_date, :summary_text, :analysis_status,
                :rule_config_version, :created_at)
    """

    with engine.begin() as conn:
        conn.execute(
            text(query),
            {
                "trade_date": trade_date,
                "summary_text": summary_text,
                "analysis_status": analysis_status,
                "rule_config_version": rule_config_version,
                "created_at": datetime.utcnow()
            }
        )