# nsepredictionbot.py

import pandas as pd, numpy as np
from datetime import datetime, timedelta, time
from pathlib import Path
import joblib
import pytz
import sys
from utils.nseholiday import nseholiday

print("----- NSE FORWARD PIPELINE START -----")

# ------------------------------------------------
# 1) Read folder + file date from backward pipeline
# ------------------------------------------------

try:
    with open("backward_date.txt") as f:
        content = f.read().strip()
        folder_date, file_date = content.split(",")

    print(f"Backward pipeline folder_date: {folder_date}")
    print(f"Backward pipeline file_date: {file_date}")

except Exception as e:

    print("ERROR: backward_date.txt not found.")
    print("Forward pipeline cannot continue.")
    sys.exit(1)

# ------------------------------------------------
# Holiday protection
# ------------------------------------------------

file_date_obj = datetime.strptime(file_date, "%Y%m%d").date()

if nseholiday(file_date_obj):
    print(f"{file_date_obj} was an NSE holiday - skipping forward pipeline.")
    sys.exit(0)

# ------------------------------------------------
# Prediction date (next market day)
# ------------------------------------------------

prediction_day = datetime.strptime(file_date, "%Y%m%d") + timedelta(days=1)

# Weekend adjustment
if prediction_day.weekday() == 5:
    prediction_day += timedelta(days=2)

elif prediction_day.weekday() == 6:
    prediction_day += timedelta(days=1)

prediction_str = prediction_day.strftime("%Y%m%d")

print(f"Prediction day: {prediction_day.date()}")

# ------------------------------------------------
# Paths
# ------------------------------------------------

MODEL_DIR = Path("./models")

OUTPUT_BACK_DIR = Path(f"./Output/{folder_date}/backward")
OUTPUT_FORWARD_DIR = Path(f"./Output/{folder_date}/forward")

OUTPUT_FORWARD_DIR.mkdir(parents=True, exist_ok=True)

print(f"Backward directory: {OUTPUT_BACK_DIR}")
print(f"Forward directory: {OUTPUT_FORWARD_DIR}")

summary_list = []

# ------------------------------------------------
# Load models
# ------------------------------------------------

for model_path in MODEL_DIR.glob("*_backward.pkl"):

    name = model_path.stem.replace("_backward","")

    print(f"\nProcessing model: {name}")

    m = joblib.load(model_path)

    back_csv = OUTPUT_BACK_DIR / f"nifty_{name}_backward_{file_date}.csv"

    if not back_csv.exists():

        print(f"WARNING: backward file missing - {back_csv}")
        continue

    df = pd.read_csv(back_csv, parse_dates=["Datetime"])

    latest = df.iloc[-1]

    last_close = latest["Close"]
    last_sma5 = latest["SMA_5"]
    last_sma20 = latest["SMA_20"]
    last_rsi = latest["RSI"]
    last_atr = latest["ATR"]

    print(f"Latest close used for prediction: {last_close}")

    # ------------------------------------------------
    # Generate 5-minute timestamps for next market day
    # ------------------------------------------------

    times = pd.date_range(
        start=datetime.combine(prediction_day.date(), time(9,15)),
        end=datetime.combine(prediction_day.date(), time(15,30)),
        freq="5min",
        tz=pytz.timezone("Asia/Kolkata")
    )

    print(f"Total intraday bars to predict: {len(times)}")

    records = []

    for ts in times:

        X = pd.DataFrame([{
            "SMA_5": last_sma5,
            "SMA_20": last_sma20,
            "RSI": last_rsi,
            "ATR": last_atr,
            "Close": last_close
        }])

        direction = int(m.predict(X)[0])

        pct = abs(latest["Pct_Change"]) / 100

        pred_price = last_close*(1+pct) if direction else last_close*(1-pct)

        records.append({
            "Datetime": ts,
            "Close_Price": last_close,
            "Prediction": direction,
            "Predicted_Price": pred_price
        })

    pred_df = pd.DataFrame(records)

    output_file = OUTPUT_FORWARD_DIR / f"nifty_{name}_forward_{folder_date}.csv"

    pred_df.to_csv(output_file, index=False)

    print(f"Saved forward prediction: {output_file}")

    # ------------------------------------------------
    # Summary metrics
    # ------------------------------------------------

    num_total = len(pred_df)

    num_bullish = pred_df["Prediction"].sum()

    num_bearish = num_total - num_bullish

    bullish_pct = num_bullish / num_total * 100

    summary_data = {

        "model": name,
        "date": prediction_day.strftime("%Y-%m-%d"),
        "total_bars": num_total,
        "bullish": num_bullish,
        "bearish": num_bearish,
        "bullish_pct": round(bullish_pct,2),
        "predicted_close_mean": round(pred_df["Predicted_Price"].mean(),2)
    }

    summary_list.append(summary_data)

# ------------------------------------------------
# Save summary
# ------------------------------------------------

summary_df = pd.DataFrame(summary_list)

summary_file = OUTPUT_FORWARD_DIR / f"forward_summary_{folder_date}.csv"

summary_df.to_csv(summary_file,index=False)

print(f"Forward summary saved: {summary_file}")

# ------------------------------------------------
# Save folder date for GitHub Action
# ------------------------------------------------

with open("predicton_date.txt","w") as f:
    f.write(folder_date)

print("predicton_date.txt written for GitHub Actions")

print("----- NSE FORWARD PIPELINE COMPLETE -----")