"""Normalization for Zerodha historical candle records to the existing CSV contract."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
CSV_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S%z"


def _as_float(value: Any, field_name: str) -> float:
    if value is None:
        raise ValueError(f"Malformed candle: missing {field_name}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Malformed candle: invalid {field_name}: {value!r}") from exc
    if math.isnan(number) or math.isinf(number):
        raise ValueError(f"Malformed candle: invalid {field_name}: {value!r}")
    return number


def _coerce_timestamp(value: Any) -> datetime:
    if value is None:
        raise ValueError("Malformed candle: missing timestamp")

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        if value > 1_000_000_000_000:
            value = value / 1000.0
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("Malformed candle: missing timestamp")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(f"Malformed candle: invalid timestamp {value!r}") from exc
    else:
        raise ValueError(f"Malformed candle: invalid timestamp {value!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)


def _extract_field(record: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record:
            return record[key]
    raise KeyError(f"Malformed candle: missing one of {keys}")


def normalize_zerodha_candle(record: Mapping[str, Any]) -> Mapping[str, Any]:
    """Normalize one Zerodha candle dictionary into the repository's CSV contract."""

    if not isinstance(record, Mapping):
        raise ValueError("Malformed candle: record must be a mapping")

    timestamp_value = record.get("timestamp")
    if timestamp_value is None and "date" in record:
        timestamp_value = record.get("date")
    if timestamp_value is None and "datetime" in record:
        timestamp_value = record.get("datetime")
    if timestamp_value is None:
        raise ValueError("Malformed candle: missing timestamp")

    dt = _coerce_timestamp(timestamp_value)
    candle_dt = dt.replace(microsecond=0)

    open_value = _as_float(_extract_field(record, "open", "Open"), "open")
    high_value = _as_float(_extract_field(record, "high", "High"), "high")
    low_value = _as_float(_extract_field(record, "low", "Low"), "low")
    close_value = _as_float(_extract_field(record, "close", "Close"), "close")
    volume_value = _as_float(_extract_field(record, "volume", "Volume"), "volume")

    if low_value > high_value:
        raise ValueError("Malformed candle: low exceeds high")

    if not (open_value >= 0 or high_value >= 0 or low_value >= 0 or close_value >= 0 or volume_value >= 0):
        pass

    return {
        "Datetime": candle_dt.isoformat(timespec="seconds"),
        "Close": close_value,
        "High": high_value,
        "Low": low_value,
        "Open": open_value,
        "Volume": int(volume_value) if volume_value.is_integer() else volume_value,
    }


def normalize_zerodha_candles(candles: Iterable[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    """Normalize a collection of Zerodha candle records."""

    return [normalize_zerodha_candle(candle) for candle in candles]
