# nseevaluationscript.py

import pandas as pd
import numpy as np
from datetime import datetime
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

except Exception as e:

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

BACKWARD_DIR = OUTPUT_DIR / folder_date / "backward"
FORWARD_DIR  = OUTPUT_DIR / folder_date / "forward"
EVAL_DIR     = OUTPUT_DIR / folder_date / "evaluation"

EVAL_DIR.mkdir(parents=True, exist_ok=True)

print(f"Backward directory: {BACKWARD_DIR}")
print(f"Forward directory: {FORWARD_DIR}")
print(f"Evaluation directory: {EVAL_DIR}")

summary_rows = []

# ------------------------------------------------
# Evaluate each model
# ------------------------------------------------

for backward_file in BACKWARD_DIR.glob("nifty_*_*.csv"):

    model_name = backward_file.stem.split("_")[1]

    forward_file = FORWARD_DIR / f"nifty_{model_name}_forward_{folder_date}.csv"

    print(f"\nProcessing model: {model_name}")

    print(f"Backward file: {backward_file}")
    print(f"Forward file: {forward_file}")

    if not forward_file.exists():

        print(f"WARNING: Missing forward file for model: {model_name}")
        continue

    # ------------------------------------------------
    # Load files
    # ------------------------------------------------

    actual_df = pd.read_csv(backward_file, parse_dates=["Datetime"])
    pred_df   = pd.read_csv(forward_file, parse_dates=["Datetime"])

    print("Forward file preview:")
    print(pred_df.head().to_string(index=False))

    print("Backward file preview:")
    print(actual_df.head().to_string(index=False))

    # ------------------------------------------------
    # Prepare data
    # ------------------------------------------------

    pred_df = pred_df.rename(columns={
        "Close_Price": "close_price_pred",
        "Predicted_Price": "predicted_price"
    })

    actual_df = actual_df.rename(columns={
        "Close": "close_price_act",
        "Target": "true_direction"
    })[["Datetime","close_price_act","true_direction"]]

    pred_df["Datetime"] = pd.to_datetime(pred_df["Datetime"]).dt.tz_localize(None)
    actual_df["Datetime"] = pd.to_datetime(actual_df["Datetime"]).dt.tz_localize(None)

    # ------------------------------------------------
    # Filter actuals to forward timestamps
    # ------------------------------------------------

    filtered_actual_df = actual_df[
        actual_df["Datetime"].isin(pred_df["Datetime"])
    ]

    print("Filtered actual rows preview:")
    print(filtered_actual_df.head().to_string(index=False))

    # ------------------------------------------------
    # Merge
    # ------------------------------------------------

    cmp = pd.merge(pred_df, filtered_actual_df, on="Datetime", how="inner")

    cmp["was_correct"] = cmp["Prediction"] == cmp["true_direction"]

    cmp["error_mag"] = np.abs(
        cmp["predicted_price"] - cmp["close_price_act"]
    )

    if cmp.empty:

        print(f"No matching records found for model: {model_name}")
        continue

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

summary_df.to_csv(summary_file, index=False)

print(f"\nEvaluation summary saved: {summary_file}")

# ------------------------------------------------
# Write folder date for GitHub Action
# ------------------------------------------------

with open("evaluation_date.txt","w") as f:

    f.write(folder_date)

print("evaluation_date.txt written for GitHub Actions")

print("----- NSE EVALUATION PIPELINE COMPLETE -----")