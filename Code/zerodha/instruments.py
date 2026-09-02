#/nsetradingbot/Code/zerodha/instruments.py
"""Instrument-list helpers for Zerodha NIFTY Futures selection."""

from __future__ import annotations

from typing import Any, Iterable, List, Mapping


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def is_nifty_futures_instrument(record: Mapping[str, Any]) -> bool:
    """Return True when the instrument record matches NIFTY futures metadata.

    The logic uses the instrument metadata itself instead of hard-coded monthly
    names or calendar assumptions.
    """

    if not isinstance(record, Mapping):
        return False

    exchange = _as_text(record.get("exchange")).upper()
    segment = _as_text(record.get("segment")).upper()
    instrument_type = _as_text(record.get("instrument_type")).upper()
    tradingsymbol = _as_text(record.get("tradingsymbol")).upper()
    name = _as_text(record.get("name")).upper()
    expiry = record.get("expiry")

    if exchange not in {"NFO", "NFO-OPT"}:
        return False
    if instrument_type != "FUT":
        return False
    if expiry in (None, ""):
        return False
    if not (tradingsymbol or name):
        return False

    symbol_text = f"{tradingsymbol} {name}"
    if "NIFTY" not in symbol_text:
        return False

    # Keep the contract classification driven by metadata instead of forcing a
    # hard-coded monthly contract name list.
    if segment and segment != "NFO-FUT":
        return False

    return True


def filter_nifty_futures(instruments: Iterable[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    """Return only NIFTY futures instruments from a Zerodha raw instrument list."""

    filtered: List[Mapping[str, Any]] = []
    for instrument in instruments or []:
        if is_nifty_futures_instrument(instrument):
            filtered.append(instrument)
    return filtered
