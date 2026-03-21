# nse_nifty_data_download.py
# Download Nifty 5m data and store clean candles for intraday system

import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import requests
import time, random
from utils.nseholiday import nseholiday
from zoneinfo import ZoneInfo

# -------------------------------
# Parameters
# -------------------------------

INTERVAL = "5m"

print("----- NIFTY DATA DOWNLOAD START -----")

# Ensure IST timezone (important for GitHub runners)
now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
print(f"Pipeline run time (IST): {now_ist}")

today = now_ist

# Weekend protection
if today.weekday() == 5:
    print("Today is Saturday - rolling back to Friday")
    today -= timedelta(days=1)

elif today.weekday() == 6:
    print("Today is Sunday - rolling back to Friday")
    today -= timedelta(days=2)

end_date = today
start_date = end_date - timedelta(days=10)

start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

print(f"Data download window: {start_str} - {end_str}")

# -------------------------------
# Yahoo Chart API Patch
# -------------------------------

def fetch_chart_data(symbol, start_epoch, end_epoch, interval):

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    params = {
        "period1": start_epoch,
        "period2": end_epoch,
        "interval": interval,
        "events": "history",
        "includePrePost": "false",
    }

    try:

        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()

        data = r.json()
        result = data["chart"]["result"][0]

        timestamps = result["timestamp"]
        indicators = result["indicators"]["quote"][0]

        df = pd.DataFrame(
            {
                "Open": indicators["open"],
                "High": indicators["high"],
                "Low": indicators["low"],
                "Close": indicators["close"],
                "Volume": indicators["volume"],
            },
            index=pd.to_datetime(timestamps, unit="s", utc=True).tz_convert("Asia/Kolkata"),
        )

        return df.dropna()

    except Exception as e:

        print(f"Chart API patch error: {e}")
        return pd.DataFrame()


# -------------------------------
# Main Data Downloader
# -------------------------------

def get_nse_data(symbol, start_str, end_str, interval):

    print("Downloading bulk data from Yahoo Finance...")

    df_bulk = yf.download(
        symbol,
        start=start_str,
        end=end_str,
        interval=interval,
        progress=False,
        threads=True,
    )

    if isinstance(df_bulk.columns, pd.MultiIndex):
        df_bulk.columns = [c[0] for c in df_bulk.columns]

    if df_bulk.index.tz is None:
        df_bulk.index = df_bulk.index.tz_localize("Asia/Kolkata")
    else:
        df_bulk.index = df_bulk.index.tz_convert("Asia/Kolkata")

    missing_times = df_bulk[df_bulk.isna().any(axis=1)].index

    if len(missing_times) == 0:
        print("No missing intervals detected.")
        return df_bulk

    print(f"Found {len(missing_times)} missing intervals - attempting patch...")

    patched_rows = []

    for ts in missing_times:

        time.sleep(random.uniform(2,4))

        start_epoch = int(ts.timestamp()) - 60
        end_epoch = int(ts.timestamp()) + 60

        df_patch = fetch_chart_data(symbol, start_epoch, end_epoch, interval)

        if not df_patch.empty:

            if ts in df_patch.index:

                row = df_patch.loc[ts]

                patched_rows.append(
                    {
                        "Datetime": ts,
                        "Open": row["Open"],
                        "High": row["High"],
                        "Low": row["Low"],
                        "Close": row["Close"],
                        "Volume": row["Volume"],
                    }
                )

    if patched_rows:

        print(f"Patched {len(patched_rows)} missing candles")

        df_patch_final = pd.DataFrame(patched_rows).set_index("Datetime")
        df_bulk.update(df_patch_final)

    return df_bulk


# -------------------------------
# Data Fetch + Validation
# -------------------------------

MAX_RETRIES = 3

for attempt in range(MAX_RETRIES):

    print(f"\nData fetch attempt {attempt+1}/{MAX_RETRIES}")

    nifty = get_nse_data("^NSEI", start_str=start_str, end_str=end_str, interval="5m")

    if isinstance(nifty.columns, pd.MultiIndex):
        nifty.columns = [c[0] for c in nifty.columns]

    last_candle_date = nifty.index[-1].date()

    print(f"Last candle detected from Yahoo: {last_candle_date}")

    # Holiday safety
    if nseholiday(last_candle_date):

        print(f"{last_candle_date} is an NSE holiday - skipping pipeline.")
        exit(0)

    print("Yahoo data accepted.")
    break

else:

    raise RuntimeError("Yahoo data fetch failed after retries.")

# -------------------------------
# Output Folder
# -------------------------------

file_date = last_candle_date.strftime("%Y%m%d")

print(f"File date (market session): {file_date}")

OUTPUT_DIR = Path(f"./Output/{file_date}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

output_file = OUTPUT_DIR / f"{file_date}data.csv"

# -------------------------------
# Save Clean Data
# -------------------------------

# ✅ Added sorting
nifty = nifty.sort_index()

nifty = nifty.reset_index().dropna()

print(f"Total rows downloaded: {len(nifty)}")

nifty.rename(columns={"index":"Datetime"}, inplace=True)

nifty.to_csv(output_file, index=False)

print(f"Nifty data saved: {output_file}")

# ✅ Added GitHub integration file
with open("download_date.txt","w") as f:
    f.write(file_date)

print("download_date.txt written for GitHub Actions")

print("----- NIFTY DATA DOWNLOAD COMPLETE -----")