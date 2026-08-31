#/nsetradingbot/Code/zerodha/historical.py
"""Generic Zerodha historical candle wrapper for NIFTY Futures data."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, List, Mapping

from .normalization import normalize_zerodha_candle


ALLOWED_INTERVALS = {
    "5minute": "5minute",
    "5m": "5minute",
    "5": "5minute",
    "5min": "5minute",
}


def validate_interval(interval: Any) -> str:
    if interval is None:
        raise ValueError("Invalid interval: None")

    normalized = str(interval).strip().lower().replace(" ", "")
    if normalized not in ALLOWED_INTERVALS:
        raise ValueError(f"Invalid interval: {interval}. Expected 5-minute data.")
    return ALLOWED_INTERVALS[normalized]


def fetch_historical_candles(
    client: Any,
    instrument_token: Any,
    from_datetime: Any,
    to_datetime: Any,
    interval: Any = "5minute",
) -> List[Mapping[str, Any]]:
    """Fetch raw historical candles from a Zerodha-compatible client.

    This module intentionally does not write CSV or perform DB work. It only
    wraps the historical_data call and normalizes the raw response shape.
    """

    if instrument_token in (None, ""):
        raise ValueError("Missing instrument_token")

    if from_datetime is None or to_datetime is None:
        raise ValueError("Invalid date range")

    if isinstance(from_datetime, str):
        from_datetime = datetime.fromisoformat(from_datetime)
    if isinstance(to_datetime, str):
        to_datetime = datetime.fromisoformat(to_datetime)

    if from_datetime >= to_datetime:
        raise ValueError("Invalid date range: from_datetime must be earlier than to_datetime")

    interval_name = validate_interval(interval)

    if client is None or not hasattr(client, "historical_data"):
        raise NotImplementedError(
            "KiteConnect client is not available yet. The historical wrapper is "
            "prepared for later injection of a real client."
        )

    raw_candles = client.historical_data(
        instrument_token,
        from_datetime,
        to_datetime,
        interval_name,
    )

    normalized: List[Mapping[str, Any]] = []
    if raw_candles is None:
        return normalized

    if isinstance(raw_candles, dict):
        raw_candles = [raw_candles]

    for candle in raw_candles:
        normalized.append(normalize_zerodha_candle(candle))

    return normalized
