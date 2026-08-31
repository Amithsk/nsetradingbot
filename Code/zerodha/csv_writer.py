#/nsetradingbot/Code/zerodha/csv_writer.py
"""Reusable CSV writer for Zerodha NIFTY Futures output."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Mapping, Sequence

CSV_COLUMNS: Sequence[str] = [
    "Datetime",
    "Close",
    "High",
    "Low",
    "Open",
    "Volume",
]


def write_nifty_futures_csv(
    records: Iterable[Mapping[str, object]],
    file_path: str | Path,
) -> Path:
    """Write NIFTY Futures candle rows to a CSV using the repo's contract."""

    target = Path(file_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        for record in records:
            row = {key: record.get(key, "") for key in CSV_COLUMNS}
            writer.writerow(row)

    return target
