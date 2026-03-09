# nseevaluationscript.py

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
import sys
from utils.nseholiday import nseholiday

print("----- NSE EVALUATION PIPELINE START -----")

OUTPUT_DIR = Path("./Output")

# ------------------------------------------------
# Read folder date from backward pipeline
# ------------------------------------------------

try:

    with open("backward_date.txt") as f:
        content = f.read().strip()
        folder_date, file_date = content.split(",")

    print(f"Backward pipeline folder_date: {folder_date}")
    print(f"Backward pipeline file_date: {file_date}")

except Exception:

    print("ERROR: backward_date.txt missing - cannot run evaluation.")
    sys.exit(1)

# ------------------------------------------------
# Holiday protection
# ------------------------------------------------

file_date_obj = datetime.strptime(file_date, "%Y%m%d").date()

if nseholiday(file_date_obj):

    print(f"{file_date_obj} was an NSE holiday - skipping evaluation.")
    sys.exit(0)

# ------------------------------------------------
# Paths
# ------------------------------------------------

FORWARD_DIR  = OUTPUT_DIR / folder_date / "forward"
EVAL_DIR     = OUTPUT_DIR / folder_date / "evaluation"

EVAL_DIR.mkdir(parents=True, exist_ok=True)

print(f"Forward directory: {FORWARD_DIR}")
print(f"Evaluation directory: {EVAL_DIR}")

summary_rows = []

# ------------------------------------------------
# Download ACTUAL candles from Yahoo
# ------------------------------------------------

eval_date = datetime.strptime(folder_date, "%Y%m%d").date()

start_date = eval_date.strftime("%Y-%m-%d")
end_date   = (eval_date + timedelta(days=1)).strftime("%Y-%m-%d")

print(f"Downloading actual candles from Yahoo for {start_date}")

try:

    actual_df = yf.download(
        "^NSEI",
        start=start_date,
        end=end_date,
        interval="5m",
        progress=False
    )

except Exception as e:

    print(f"ERROR downloading Yahoo data: {e}")
    sys.exit(1)

if actual_df.empty:

    print("Yahoo returned empty dataset - skipping evaluation.")
    sys.exit(0)

# ------------------------------------------------
# FIX: Yahoo sometimes returns MultiIndex columns
# ------------------------------------------------

if isinstance(actual_df.columns, pd.MultiIndex):
    actual_df.columns = actual_df.columns.get_level_values(0)

# ------------------------------------------------

actual_df = actual_df.reset_index()

actual_df.rename(columns={
    "Close": "close_price_act"
}, inplace=True)

actual_df["Datetime"] = pd.to_datetime(actual_df["Datetime"]).dt.tz_localize(None)

# true direction calculation
actual_df["true_direction"] = (
    actual_df["close_price_act"].shift(-1) > actual_df["close_price_act"]
).astype(int)

actual_df.dropna(inplace=True)

print(f"Downloaded {len(actual_df)} candles")

# ------------------------------------------------
# Evaluate each model
# ------------------------------------------------

for forward_file in FORWARD_DIR.glob("nifty_*_forward_*.csv"):

    model_name = forward_file.stem.split("_")[1]

    print(f"\nProcessing model: {model_name}")

    print(f"Forward file: {forward_file}")

    # ------------------------------------------------
    # Load prediction file
    # ------------------------------------------------

    pred_df = pd.read_csv(forward_file, parse_dates=["Datetime"])

    print("Forward file preview:")
    print(pred_df.head().to_string(index=False))

    pred_df = pred_df.rename(columns={
        "Close_Price": "close_price_pred",
        "Predicted_Price": "predicted_price"
    })

    pred_df["Datetime"] = pd.to_datetime(pred_df["Datetime"]).dt.tz_localize(None)

    # ------------------------------------------------
    # Merge predictions with actual candles
    # ------------------------------------------------

    cmp = pd.merge(
        pred_df,
        actual_df[["Datetime","close_price_act","true_direction"]],
        on="Datetime",
        how="inner"
    )

    if cmp.empty:

        print(f"No matching candles found for model: {model_name}")
        continue

    cmp["was_correct"] = cmp["Prediction"] == cmp["true_direction"]

    cmp["error_mag"] = np.abs(
        cmp["predicted_price"] - cmp["close_price_act"]
    )

    # ------------------------------------------------
    # Save comparison
    # ------------------------------------------------

    comparison_file = EVAL_DIR / f"{model_name}_comparison_{folder_date}.csv"

    cmp.to_csv(comparison_file, index=False)

    print(f"Saved comparison file: {comparison_file}")

    print("Merged comparison preview:")
    print(cmp.head().to_string(index=False))

    # ------------------------------------------------
    # Summary metrics
    # ------------------------------------------------

    total     = len(cmp)
    correct   = cmp["was_correct"].sum()
    incorrect = total - correct

    accuracy = correct / total * 100 if total else np.nan

    avg_error = (
        cmp.loc[~cmp["was_correct"], "error_mag"].mean()
        if incorrect else 0.0
    )

    summary_rows.append({

        "model": model_name,
        "date": file_date_obj,
        "total_bars": total,
        "correct": correct,
        "incorrect": incorrect,
        "accuracy_pct": round(accuracy,2),
        "avg_error_mag": round(avg_error,4)

    })

# ------------------------------------------------
# Save summary
# ------------------------------------------------

summary_df = pd.DataFrame(summary_rows)

summary_file = EVAL_DIR / f"evaluation_summary_{folder_date}.csv"
if not summary_df.empty:

    summary_df.to_csv(summary_file, index=False)

    print(f"\nEvaluation summary saved: {summary_file}")

else:

    print("No evaluation results generated — summary file not created.")


# ------------------------------------------------
# Write folder date for GitHub Action
# ------------------------------------------------

with open("evaluation_date.txt","w") as f:

    f.write(folder_date)

print("evaluation_date.txt written for GitHub Actions")

print("----- NSE EVALUATION PIPELINE COMPLETE -----")