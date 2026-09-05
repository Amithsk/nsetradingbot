"""FastAPI endpoints for Zerodha market data.

This module exposes Zerodha data through the API layer.
It reuses the existing Zerodha modules and does not perform
analytics or modify the existing download flow.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from fastapi import FastAPI, HTTPException

from .config import get_kite_client
from .nifty_futures import select_nifty_futures_contract
from .historical import fetch_historical_candles


app = FastAPI()


# ------------------------------------------------
# CONFIGURATION
# ------------------------------------------------

INTERVAL = "5minute"


# ------------------------------------------------
# TRADING DATE
# ------------------------------------------------

def get_target_date() -> date:
    """Return today's date.

    The API uses the current market date supplied by the caller's
    environment. Weekend handling is intentionally kept consistent
    with the existing download flow.
    """

    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")

    today = datetime.now(IST).date()

    if today.weekday() == 5:
        today -= timedelta(days=1)

    elif today.weekday() == 6:
        today -= timedelta(days=2)

    return today


# ------------------------------------------------
# DATA RANGE
# ------------------------------------------------

def get_candle_range(
    target_date: date,
) -> tuple[datetime, datetime]:
    """Return the Zerodha historical-data range for the target date."""

    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")

    from_datetime = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        9,
        15,
        tzinfo=IST,
    )

    to_date = target_date + timedelta(days=1)

    to_datetime = datetime(
        to_date.year,
        to_date.month,
        to_date.day,
        0,
        0,
        tzinfo=IST,
    )

    return from_datetime, to_datetime


# ------------------------------------------------
# NIFTY FUTURES CANDLES
# ------------------------------------------------

@app.get("/zerodha/nifty-futures/candles")
def nifty_futures_candles() -> dict[str, Any]:
    """Return normalized NIFTY Futures 5-minute candles from Zerodha."""

    try:
        # ------------------------------------------------
        # TARGET DATE
        # ------------------------------------------------

        target_date = get_target_date()

        # ------------------------------------------------
        # KITE CLIENT
        # ------------------------------------------------

        kite = get_kite_client()

        # ------------------------------------------------
        # INSTRUMENT MASTER
        # ------------------------------------------------

        instruments = kite.instruments("NFO")

        # ------------------------------------------------
        # CONTRACT SELECTION
        # ------------------------------------------------

        contract = select_nifty_futures_contract(
            target_date,
            instruments,
        )

        # ------------------------------------------------
        # HISTORICAL DATA RANGE
        # ------------------------------------------------

        from_datetime, to_datetime = get_candle_range(
            target_date
        )

        # ------------------------------------------------
        # HISTORICAL CANDLES
        # ------------------------------------------------

        records = fetch_historical_candles(
            client=kite,
            instrument_token=contract["instrument_token"],
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            interval=INTERVAL,
        )

        # ------------------------------------------------
        # RESPONSE
        # ------------------------------------------------

        return {
            "status": "success",
            "trade_date": target_date.isoformat(),
            "interval": INTERVAL,
            "contract": contract,
            "candles": records,
            "count": len(records),
        }

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Unable to fetch NIFTY Futures candles: {exc}",
        ) from exc