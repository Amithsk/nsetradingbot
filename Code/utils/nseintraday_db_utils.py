
import sqlalchemy as sa
import pandas as pd
from typing import Dict

# canonical keys we expect in code:
CANONICAL = {
    "symbol": ["symbol", "security", "sym"],
    "trade_date": ["trade_date", "tradeDate", "trade_day", "file_date", "as_of_date"],
    "open": ["open", "open_price", "open_prc", "open_pr"],
    "high": ["high", "high_price", "hi_price"],
    "low": ["low", "low_price", "lo_price"],
    "close": ["close", "close_price", "close_pric", "close_pr"],
    "prev_close": ["prev_close", "prev_cl_pr", "prev_close_price", "prevclose"],
    "net_trdval": ["net_trdval", "net_trd_val", "net_trdvalrs", "net_trdvalue"],
    "net_trdqty": ["net_trdqty", "net_trd_qty", "net_trdqty"],
    "trades": ["trades", "no_of_trades"],
    "mkt_flag": ["mkt_flag", "mkt", "mkt_ind"],
    "ind_sec": ["ind_sec", "industry_sector", "indsec"],
    "corp_ind": ["corp_ind", "corp_industry"],
    "hi_52_wk": ["hi_52_wk", "52wk_hi", "hi_52wk"],
    "lo_52_wk": ["lo_52_wk", "52wk_lo", "lo_52wk"]
}

def detect_intraday_columns(engine: sa.engine.Engine) -> Dict[str, str]:
    """Return mapping canonical_key -> actual_column_name in intraday_bhavcopy.
    Raises RuntimeError if required core OHLC/date columns can't be found.
    """
    inspector = sa.inspect(engine)
    try:
        cols_info = inspector.get_columns('intraday_bhavcopy')
        actual_cols = {c['name'].lower(): c['name'] for c in cols_info}
    except Exception:
        # fallback to information_schema query
        q = sa.text("""
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'intraday_bhavcopy'
        """)
        with engine.connect() as conn:
            rows = conn.execute(q).fetchall()
            actual_cols = {r[0].lower(): r[0] for r in rows}

    found = {}
    for canon, candidates in CANONICAL.items():
        for c in candidates:
            if c.lower() in actual_cols:
                found[canon] = actual_cols[c.lower()]
                break
    # Verify essentials
    essentials = ["symbol", "trade_date", "open", "high", "low", "close"]
    missing = [e for e in essentials if e not in found]
    if missing:
        raise RuntimeError(f"Could not detect required intraday_bhavcopy columns: {missing}")
    return found
