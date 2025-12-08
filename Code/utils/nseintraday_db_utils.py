"""
nseintraday_db_utils.py

1/Utility to detect intraday_bhavcopy column names (common variants).
    Robust: tries SQLAlchemy inspector, falls back to information_schema.
2/Connect to intradaytrading DB 

"""
import sqlalchemy as sa
import logging
from typing import Dict

logger = logging.getLogger("nseintraday_db_utils")

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
    Uses inspector when possible, otherwise falls back to information_schema.
    Raises RuntimeError if essential OHLC/date columns not found.
    """
    try:
        inspector = sa.inspect(engine)
        cols_info = inspector.get_columns('intraday_bhavcopy')
        actual_cols = {c['name'].lower(): c['name'] for c in cols_info}
    except Exception as e:
        # inspector failed (auth/permission) — fallback to information_schema
        try:
            safe_url = engine.url.render_as_string(hide_password=True)
        except Exception:
            safe_url = "<engine-url-unavailable>"
        logger.debug("Inspector failed for engine %s: %s. Falling back to information_schema.", safe_url, e)

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

    essentials = ["symbol", "trade_date", "open", "high", "low", "close"]
    missing = [e for e in essentials if e not in found]
    if missing:
        raise RuntimeError(f"Could not detect required intraday_bhavcopy columns: {missing}")
    return found


# -------------------------
# DB connection helper
# -------------------------

import sqlalchemy as sa
import logging
from typing import Dict
import urllib.parse
import os
from sqlalchemy import create_engine, text

# Setup logger for this module
logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "db": {
        "host": "localhost",
        "user": "root",
        "password": "",
        "db": "intradaytrading",
        "port": 3306
    }
}

def connect_db(db_cfg: dict = None):
    """
    Create and return a SQLAlchemy engine.

    Priority of credentials:
      1. MYSQL_PASSWORD environment variable
      2. db_cfg["password"]
      3. DEFAULT password
    """
    
    cfg = db_cfg if db_cfg else DEFAULT_CONFIG["db"]

    # Password priority: environment > config
    env_password = os.getenv("MYSQL_PASSWORD")
    password = env_password if env_password else cfg.get("password", "")
    encoded_pw = urllib.parse.quote_plus(password)

    user = cfg.get("user", "root")
    host = cfg.get("host", "localhost")
    port = cfg.get("port", 3306)
    dbname = cfg.get("db", "intradaytrading")

    DATABASE_URL = f"mysql+pymysql://{user}:{encoded_pw}@{host}:{port}/{dbname}"

    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )

    logger.info(f"SQLAlchemy engine created for DB: {dbname}")
    return engine
