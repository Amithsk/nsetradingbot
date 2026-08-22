"""Zerodha provider foundation for NIFTY Futures data collection.

This package is intentionally isolated and does not modify the current
Yahoo NIFTY collector, NSE pipeline, analytics engine, database, or cron.
"""

from .config import KiteConfig, get_kite_client, load_kite_config
from .instruments import filter_nifty_futures, is_nifty_futures_instrument
from .nifty_futures import (
    NoApplicableContractFoundError,
    NoNiftyFuturesFoundError,
    MissingExpiryError,
    select_nifty_futures_contract,
)
from .historical import fetch_historical_candles, validate_interval
from .normalization import normalize_zerodha_candles
from .csv_writer import CSV_COLUMNS, write_nifty_futures_csv

__all__ = [
    "KiteConfig",
    "get_kite_client",
    "load_kite_config",
    "filter_nifty_futures",
    "is_nifty_futures_instrument",
    "NoApplicableContractFoundError",
    "NoNiftyFuturesFoundError",
    "MissingExpiryError",
    "select_nifty_futures_contract",
    "fetch_historical_candles",
    "validate_interval",
    "normalize_zerodha_candles",
    "CSV_COLUMNS",
    "write_nifty_futures_csv",
]
