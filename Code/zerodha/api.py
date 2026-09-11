#/nseradingbot/Code/zerodha/api.py
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
# GENERIC SYMBOL CANDLES
# ------------------------------------------------

@app.get("/zerodha/symbol/candles")
def symbol_candles(symbol: str) -> dict[str, Any]:
    """Return normalized 5-minute candles for a Zerodha symbol.

    Example:
        /zerodha/symbol/candles?symbol=NSE:SBIN

    The trading date is determined on the Pi using IST.
    """

    try:
        # ------------------------------------------------
        # VALIDATE SYMBOL
        # ------------------------------------------------

        symbol = symbol.strip().upper()

        if not symbol:
            raise HTTPException(
                status_code=400,
                detail="Symbol is required. Example: NSE:SBIN",
            )

        if ":" not in symbol:
            raise HTTPException(
                status_code=400,
                detail="Symbol must include exchange. Example: NSE:SBIN",
            )

        exchange, tradingsymbol = symbol.split(":", 1)

        if not exchange or not tradingsymbol:
            raise HTTPException(
                status_code=400,
                detail="Invalid symbol. Example: NSE:SBIN",
            )

        # ------------------------------------------------
        # TARGET DATE
        # ------------------------------------------------

        target_date = get_target_date()

        # ------------------------------------------------
        # KITE CLIENT
        # ------------------------------------------------

        kite = get_kite_client()

        # ------------------------------------------------
        # INSTRUMENT
        # ------------------------------------------------

        instruments = kite.instruments(exchange)

        matching_instruments = [
            instrument
            for instrument in instruments
            if (
                str(instrument.get("exchange", "")).upper()
                == exchange
                and str(instrument.get("tradingsymbol", "")).upper()
                == tradingsymbol
            )
        ]

        if not matching_instruments:
            raise HTTPException(
                status_code=404,
                detail=f"Symbol not found: {symbol}",
            )

        instrument = matching_instruments[0]

        instrument_token = instrument.get("instrument_token")

        if not instrument_token:
            raise HTTPException(
                status_code=500,
                detail=f"Instrument token not found for {symbol}",
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
            instrument_token=instrument_token,
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
            "symbol": symbol,
            "instrument_token": instrument_token,
            "candles": records,
            "count": len(records),
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to fetch candles for {symbol}: {exc}",
        ) from exc


# ------------------------------------------------
# HISTORICAL SYMBOL CANDLES
# ------------------------------------------------

@app.get("/zerodha/symbol/historical-candles")
def historical_symbol_candles(
    symbol: str,
    trade_date: str,
) -> dict[str, Any]:
    """Return normalized 5-minute candles for a symbol on a requested date.

    Example:
        /zerodha/symbol/historical-candles?symbol=NSE:SBIN&trade_date=2026-09-10
    """

    try:
        symbol = symbol.strip().upper()

        if not symbol:
            raise HTTPException(
                status_code=400,
                detail="Symbol is required. Example: NSE:SBIN",
            )

        if ":" not in symbol:
            raise HTTPException(
                status_code=400,
                detail="Symbol must include exchange. Example: NSE:SBIN",
            )

        exchange, tradingsymbol = symbol.split(":", 1)

        if not exchange or not tradingsymbol:
            raise HTTPException(
                status_code=400,
                detail="Invalid symbol. Example: NSE:SBIN",
            )

        try:
            target_date = datetime.strptime(
                trade_date,
                "%Y-%m-%d",
            ).date()
            if target_date.isoformat() != trade_date:
                raise ValueError("trade_date must use zero-padded values")
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="trade_date must use YYYY-MM-DD format.",
            ) from exc

        kite = get_kite_client()

        instruments = kite.instruments(exchange)

        matching_instruments = [
            instrument
            for instrument in instruments
            if (
                str(instrument.get("exchange", "")).upper()
                == exchange
                and str(instrument.get("tradingsymbol", "")).upper()
                == tradingsymbol
            )
        ]

        if not matching_instruments:
            raise HTTPException(
                status_code=404,
                detail=f"Symbol not found: {symbol}",
            )

        instrument = matching_instruments[0]
        instrument_token = instrument.get("instrument_token")

        if not instrument_token:
            raise HTTPException(
                status_code=500,
                detail=f"Instrument token not found for {symbol}",
            )

        from_datetime, to_datetime = get_candle_range(target_date)

        records = fetch_historical_candles(
            client=kite,
            instrument_token=instrument_token,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            interval=INTERVAL,
        )

        return {
            "status": "success",
            "trade_date": target_date.isoformat(),
            "interval": INTERVAL,
            "symbol": symbol,
            "instrument_token": instrument_token,
            "candles": records,
            "count": len(records),
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Unable to fetch historical candles for {symbol}: {exc}"
            ),
        ) from exc
    
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
