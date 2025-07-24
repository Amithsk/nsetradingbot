import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# --- Config ---
OUTPUT_DIR = Path("./Output")

# --- Helper: previous trading day (skip Sat/Sun) ---
def prev_trading_day(dt):
    dt -= timedelta(days=1)
    while dt.weekday() >= 5:
        dt -= timedelta(days=1)
    return dt

# --- Dates ---
today = datetime.now()
today_str = today.strftime('%Y%m%d')
prev_day = prev_trading_day(today)
prev_str = prev_day.strftime('%Y%m%d')

# --- Paths ---
BACKWARD_DIR = OUTPUT_DIR / today_str / 'backward'
FORWARD_DIR  = OUTPUT_DIR / prev_str  / 'forward'
EVAL_DIR     = OUTPUT_DIR / prev_str / 'evaluation'
EVAL_DIR.mkdir(parents=True, exist_ok=True)

summary_rows = []

# --- Evaluate each model ---
for backward_file in BACKWARD_DIR.glob("nifty_*_*.csv"):
    model_name = backward_file.stem.split("_")[1]
    forward_file = FORWARD_DIR / f"nifty_{model_name}_forward_{prev_str}.csv"
    

    if not forward_file.exists():
        print(f"Missing forward file for model: {model_name}")
        continue

    # Load files
    actual_df = pd.read_csv(backward_file, parse_dates=['Datetime'])
    pred_df   = pd.read_csv(forward_file, parse_dates=['Datetime'])

    # Prepare
    pred_df = pred_df.rename(columns={
        'Close_Price':    'close_price_pred',
        'Predicted_Price':'predicted_price'
    })

    actual_df = actual_df.rename(columns={
        'Close':   'close_price_act',
        'Target':  'true_direction'
    })[['Datetime','close_price_act','true_direction']]

    pred_df['Datetime'] = pd.to_datetime(pred_df['Datetime']).dt.tz_localize(None)
    actual_df['Datetime'] = pd.to_datetime(actual_df['Datetime']).dt.tz_localize(None)
    print("The forward file \n",pred_df.head().to_string(index=False))
    print("The backward file \n",actual_df.head().to_string(index=False))
    
    # Filter actuals to forward prediction timestamps
    filtered_actual_df = actual_df[actual_df['Datetime'].isin(pred_df['Datetime'])]
    print("The filtered actual file \n",filtered_actual_df.head().to_string(index=False))


    # Merge
    cmp = pd.merge(pred_df, filtered_actual_df, on='Datetime', how='inner')
    cmp['was_correct'] = cmp['Prediction'] == cmp['true_direction']
    cmp['error_mag']   = np.abs(cmp['predicted_price'] - cmp['close_price_act'])
    
    #To check if the merge is happening properly
    if cmp.empty:
        print(f"No matching records found for model: {model_name}")
        continue  # optional: skip summary generation for this model

    # Save merged comparison
    cmp.to_csv(EVAL_DIR / f"{model_name}_comparison_{prev_str}.csv", index=False)
    print("The merged file \n",cmp.head().to_string(index=False))

    # Summary
    total     = len(cmp)
    correct   = cmp['was_correct'].sum()
    incorrect = total - correct
    accuracy  = correct / total * 100 if total else np.nan
    avg_error = cmp.loc[~cmp['was_correct'], 'error_mag'].mean() if incorrect else 0.0

    summary_rows.append({
        'model': model_name,
        'date': today.date(),
        'total_bars': total,
        'correct': correct,
        'incorrect': incorrect,
        'accuracy_pct': round(accuracy, 2),
        'avg_error_mag': round(avg_error, 4)
    })

# Save daily summary
summary_df = pd.DataFrame(summary_rows)
summary_file = EVAL_DIR / f"evaluation_summary_{prev_str}.csv"
summary_df.to_csv(summary_file, index=False)
print(f"Evaluation summary saved: {summary_file}")


# Save tstr to file so GitHub Actions can access it
with open("evaluation.txt", "w") as f:
    f.write(prev_str)