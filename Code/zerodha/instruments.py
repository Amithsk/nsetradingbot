#/nsetradingbot/Code/zerodha/instruments.py
"""Instrument-list helpers for Zerodha NIFTY Futures selection."""

from __future__ import annotations

from typing import Any, Iterable, List, Mapping


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def is_nifty_futures_instrument(record: Mapping[str, Any]) -> bool:
    """Return True for standard NIFTY index futures instruments."""

    if not isinstance(record, Mapping):
        return False

    exchange = _as_text(record.get("exchange")).upper()
    segment = _as_text(record.get("segment")).upper()
    instrument_type = _as_text(record.get("instrument_type")).upper()
    name = _as_text(record.get("name")).upper()
    expiry = record.get("expiry")

    if exchange != "NFO":
        return False

    if segment != "NFO-FUT":
        return False

    if instrument_type != "FUT":
        return False

    if name != "NIFTY":
        return False

    if expiry in (None, ""):
        return False

    return True


def filter_nifty_futures(instruments: Iterable[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    """Return only NIFTY futures instruments from a Zerodha raw instrument list."""

    filtered: List[Mapping[str, Any]] = []
    for instrument in instruments or []:
        if is_nifty_futures_instrument(instrument):
            filtered.append(instrument)
    return filtered
