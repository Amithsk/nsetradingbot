#/nsetradingbot/Code/zerodha/callback.py
"""FastAPI callback endpoint for Zerodha Kite Connect authentication."""


from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request

from .config import load_kite_config, get_kite_client


from .nifty_futures import select_nifty_futures_contract
from datetime import datetime
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")

app = FastAPI()


# Runtime location for the daily access token.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOKEN_FILE = PROJECT_ROOT / "Output" / "zerodha_access_token.json"


@app.get("/")
def home():
    return {
        "status": "Zerodha callback server is running"
    }


# ------------------------------------------------
# ZERODHA STATUS
# ------------------------------------------------

@app.get("/zerodha/status")
def zerodha_status():
    """Verify that the Pi can communicate with Zerodha using the current session."""

    try:

        kite = get_kite_client()

        # Lightweight authenticated request to Zerodha.
        profile = kite.profile()

        return {
            "status": "connected",
            "zerodha": True,
            "user_id": profile.get("user_id"),
        }

    except Exception as exc:

        return {
            "status": "disconnected",
            "zerodha": False,
            "error": str(exc),
        }



# ------------------------------------------------
# NIFTY FUTURES
# ------------------------------------------------

@app.get("/zerodha/nifty-futures")
def zerodha_nifty_futures():
    """Return the currently applicable NIFTY Futures contract."""

    try:

        kite = get_kite_client()

        instruments = kite.instruments("NFO")

        contract = select_nifty_futures_contract(
            datetime.now(IST).date(),
            instruments,
        )

        return {
            "status": "success",
            "contract": contract,
        }

    except Exception as exc:

        return {
            "status": "error",
            "error": str(exc),
        }

    
# ------------------------------------------------
# ZERODHA CALLBACK
# ------------------------------------------------



@app.get("/zerodha/callback")
async def zerodha_callback(request: Request):
    params = dict(request.query_params)

    request_token = params.get("request_token")
    status = params.get("status")

    # Zerodha may redirect with a failure status.
    if status != "success":
        return {
            "status": "callback_received",
            "parameters": params,
            "authentication": "failed",
        }

    # A successful authentication must contain request_token.
    if not request_token:
        return {
            "status": "callback_received",
            "parameters": params,
            "authentication": "failed",
            "error": "request_token was not provided by Zerodha",
        }

    try:
        config = load_kite_config()
        kite = get_kite_client()

        session_data = kite.generate_session(
            request_token,
            api_secret=config.api_secret,
        )

        access_token = session_data.get("access_token")

        if not access_token:
            raise RuntimeError(
                "Kite Connect did not return an access_token"
            )

        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)

        token_payload = {
            "access_token": access_token,
        }

        TOKEN_FILE.write_text(
            json.dumps(token_payload, indent=2),
            encoding="utf-8",
        )

        return {
            "status": "authentication_successful",
            "message": "Zerodha authentication completed and access token stored",
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Zerodha authentication failed: {exc}",
        ) from exc

    