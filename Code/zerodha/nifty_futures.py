#/nsetradingbot/Code/zerodha/nifty_futures.py
"""NIFTY futures contract selection based on the Zerodha instrument list."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from .instruments import filter_nifty_futures

logger = logging.getLogger("nsetradingbot.zerodha.nifty_futures")


class ZerodhaContractSelectionError(ValueError):
    """Base contract-selection validation error."""


class NoNiftyFuturesFoundError(ZerodhaContractSelectionError):
    """Raised when no NIFTY futures records are present in the instrument list."""


class NoApplicableContractFoundError(ZerodhaContractSelectionError):
    """Raised when no valid NIFTY futures contract exists for the supplied date."""


class MissingExpiryError(ZerodhaContractSelectionError):
    """Raised when a candidate instrument does not contain expiry metadata."""


def _coerce_trade_date(trade_date: Any) -> date:
    if isinstance(trade_date, datetime):
        return trade_date.date()
    if isinstance(trade_date, date):
        return trade_date
    if isinstance(trade_date, str):
        trade_date_text = trade_date.strip()
        if not trade_date_text:
            raise ValueError("Invalid trade date: empty string")
        try:
            return datetime.fromisoformat(trade_date_text).date()
        except ValueError:
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
                try:
                    return datetime.strptime(trade_date_text, fmt).date()
                except ValueError:
                    continue
        raise ValueError(f"Invalid trade date: {trade_date_text}")
    raise ValueError(f"Invalid trade date type: {type(trade_date)!r}")


def _coerce_expiry_value(value: Any) -> date:
    if value is None:
        raise MissingExpiryError("Missing instrument expiry")

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise MissingExpiryError("Missing instrument expiry")
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
                try:
                    return datetime.strptime(text, fmt).date()
                except ValueError:
                    continue
        raise ValueError(f"Invalid expiry date: {text}")

    raise ValueError(f"Unsupported expiry format: {type(value)!r}")


def _normalize_contract_record(record: Mapping[str, Any]) -> Mapping[str, Any]:
    expiry_value = record.get("expiry")
    expiry = _coerce_expiry_value(expiry_value).isoformat()
    return {
        "instrument_token": record.get("instrument_token"),
        "tradingsymbol": record.get("tradingsymbol"),
        "expiry": expiry,
        "exchange": record.get("exchange"),
        "segment": record.get("segment"),
        "instrument_type": record.get("instrument_type"),
    }


def select_nifty_futures_contract(
    trade_date: Any,
    instruments: Iterable[Mapping[str, Any]],
) -> Mapping[str, Any]:
    """Select the nearest valid NIFTY futures contract for a trading date.

    The selection is deterministic and uses the expiry metadata from the Zerodha
    instrument list rather than hard-coded contract names.
    """

    target_date = _coerce_trade_date(trade_date)
    candidates = filter_nifty_futures(instruments)

    if not candidates:
        raise NoNiftyFuturesFoundError(
            "No NIFTY Futures instruments found in the Zerodha instrument list."
        )

    applicable = []
    for record in candidates:
        expiry_value = record.get("expiry")
        if expiry_value in (None, ""):
            raise MissingExpiryError(
                f"Missing expiry for instrument token {record.get('instrument_token')}"
            )

        expiry_date = _coerce_expiry_value(expiry_value)
        if expiry_date >= target_date:
            applicable.append((expiry_date, record))

    if not applicable:
        raise NoApplicableContractFoundError(
            f"No NIFTY Futures contract found applicable to trade date {target_date.isoformat()}"
        )

    selected_expiry, selected_record = min(
        applicable,
        key=lambda item: (
            item[0],
            int(item[1].get("instrument_token", 0) or 0),
            str(item[1].get("tradingsymbol", "")),
        ),
    )

    normalized = _normalize_contract_record(selected_record)
    logger.info(
        "Trade date: %s | Selected: %s | Instrument token: %s | Expiry: %s",
        target_date.isoformat(),
        normalized["tradingsymbol"],
        normalized["instrument_token"],
        normalized["expiry"],
    )
    return normalized
