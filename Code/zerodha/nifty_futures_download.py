#/nsetradingbot/Code/zerodha/nifty_futures_download.py

"""Download NIFTY Futures 5-minute data from Zerodha and save it
using the existing Yahoo-compatible CSV contract.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from zoneinfo import ZoneInfo

from .config import get_kite_client
from .nifty_futures import select_nifty_futures_contract
from .historical import fetch_historical_candles
from .csv_writer import write_nifty_futures_csv


# ------------------------------------------------
# CONFIGURATION
# ------------------------------------------------

IST = ZoneInfo("Asia/Kolkata")

INTERVAL = "5minute"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_ROOT = PROJECT_ROOT / "Output"


# ------------------------------------------------
# TRADING DATE
# ------------------------------------------------

def get_target_date() -> date:
    """Return today's date in IST, rolling weekends back to Friday."""

    today = datetime.now(IST).date()

    if today.weekday() == 5:
        # Saturday -> Friday
        today -= timedelta(days=1)

    elif today.weekday() == 6:
        # Sunday -> Friday
        today -= timedelta(days=2)

    return today


# ------------------------------------------------
# DATA RANGE
# ------------------------------------------------

def get_download_range(
    target_date: date,
) -> tuple[datetime, datetime]:
    """Return the Zerodha historical API range for one market session.

    Only the target trading date is requested.

    The end time is midnight of the following calendar day so the
    historical API has an unambiguous upper boundary that includes
    the complete target session.
    """

    from_datetime = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        9,
        15,
        tzinfo=IST,
    )

    next_day = target_date + timedelta(days=1)

    to_datetime = datetime(
        next_day.year,
        next_day.month,
        next_day.day,
        0,
        0,
        tzinfo=IST,
    )

    return from_datetime, to_datetime


# ------------------------------------------------
# VALIDATION
# ------------------------------------------------

def validate_candles(
    records: list[dict[str, Any]],
    target_date: date,
) -> None:
    """Validate that Zerodha returned usable candles for target date."""

    if not records:
        raise RuntimeError(
            "Zerodha returned no NIFTY Futures 5-minute candles."
        )

    required_columns = {
        "Datetime",
        "Close",
        "High",
        "Low",
        "Open",
        "Volume",
    }

    for index, record in enumerate(records):

        missing = required_columns - set(record.keys())

        if missing:
            raise RuntimeError(
                f"Normalized candle at row {index} is missing columns: "
                + ", ".join(sorted(missing))
            )

        for column in required_columns:

            if record.get(column) in (None, ""):
                raise RuntimeError(
                    f"Invalid candle at row {index}: "
                    f"missing {column}"
                )

        datetime_value = record["Datetime"]

        if isinstance(datetime_value, str):
            datetime_value = datetime.fromisoformat(datetime_value)

        if not isinstance(datetime_value, datetime):
            raise RuntimeError(
                f"Invalid Datetime at row {index}: "
                f"{datetime_value!r}"
            )

        candle_date = datetime_value.astimezone(IST).date()

        if candle_date != target_date:
            raise RuntimeError(
                f"Candle at row {index} belongs to "
                f"{candle_date}, expected {target_date}"
            )


# ------------------------------------------------
# MAIN DOWNLOAD
# ------------------------------------------------

def download_nifty_futures() -> Path:
    """Fetch one NIFTY Futures trading session and write the existing CSV format."""

    print("----- NIFTY FUTURES DATA DOWNLOAD START -----")

    # ------------------------------------------------
    # TARGET TRADING DATE
    # ------------------------------------------------

    target_date = get_target_date()

    print(f"Target date (IST): {target_date}")

    from_datetime, to_datetime = get_download_range(
        target_date
    )

    print(
        "Historical data window: "
        f"{from_datetime.isoformat()} -> "
        f"{to_datetime.isoformat()}"
    )

    # ------------------------------------------------
    # AUTHENTICATED KITE CLIENT
    # ------------------------------------------------

    print("Connecting to Zerodha...")

    kite = get_kite_client()

    print("Zerodha connection established.")

    # ------------------------------------------------
    # INSTRUMENT MASTER
    # ------------------------------------------------

    print("Fetching NFO instruments...")

    instruments = kite.instruments("NFO")

    print(
        f"NFO instruments received: {len(instruments)}"
    )

    # ------------------------------------------------
    # CONTRACT SELECTION
    # ------------------------------------------------

    contract = select_nifty_futures_contract(
        target_date,
        instruments,
    )

    instrument_token = contract["instrument_token"]
    trading_symbol = contract["tradingsymbol"]
    expiry = contract["expiry"]

    print(
        "Selected NIFTY Futures contract: "
        f"{trading_symbol} | "
        f"Token: {instrument_token} | "
        f"Expiry: {expiry}"
    )

    # ------------------------------------------------
    # HISTORICAL DATA
    # ------------------------------------------------

    print(
        f"Downloading {INTERVAL} candles for "
        f"{trading_symbol}..."
    )

    records = fetch_historical_candles(
        client=kite,
        instrument_token=instrument_token,
        from_datetime=from_datetime,
        to_datetime=to_datetime,
        interval=INTERVAL,
    )

    print(
        f"Normalized candles received: {len(records)}"
    )

    # ------------------------------------------------
    # VALIDATION
    # ------------------------------------------------

    validate_candles(
        records,
        target_date,
    )

    print(
        f"Validated {len(records)} candles "
        f"for {target_date}"
    )

    # ------------------------------------------------
    # OUTPUT FILE
    # ------------------------------------------------

    file_date = target_date.strftime("%Y%m%d")

    output_dir = OUTPUT_ROOT / file_date

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir / f"{file_date}data.csv"
    )

    # ------------------------------------------------
    # WRITE CSV
    # ------------------------------------------------

    print(f"Writing CSV: {output_file}")

    write_nifty_futures_csv(
        records,
        output_file,
    )

    print(
        f"Total rows written: {len(records)}"
    )

    print(
        "NIFTY Futures data saved successfully: "
        f"{output_file}"
    )

    print("----- NIFTY FUTURES DATA DOWNLOAD COMPLETE -----")

    return output_file


# ------------------------------------------------
# SCRIPT ENTRY POINT
# ------------------------------------------------

if __name__ == "__main__":

    try:

        output_file = download_nifty_futures()

        print()
        print("Download completed successfully.")
        print(f"Output file: {output_file}")

    except Exception as exc:

        print()
        print("NIFTY Futures data download failed.")
        print(f"Error: {exc}")

        raise