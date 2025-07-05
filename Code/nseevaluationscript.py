import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# --- Config ---
OUTPUT_DIR = Path("./Output")
OUTPUT_DIR.mkdir(exist_ok=True)

# --- Helper: previous trading day (skip Sat/Sun) ---
def prev_trading_day(dt):
    dt -= timedelta(days=1)
    while dt.weekday() >= 5:  # 5=Sat, 6=Sun
        dt -= timedelta(days=1)
    return dt

# --- Determine dates ---
today       = datetime.now()
today_str   = today.strftime("%Y%m%d")
prev_day    = prev_trading_day(today)
prev_str    = prev_day.strftime("%Y%m%d")

# --- File paths ---
# Backward evaluation for today's actual bars
back_file = OUTPUT_DIR / f"nifty_xgb_backward_{today_str}.csv"
# Forward predictions generated yesterday for today's bars
fwd_file  = OUTPUT_DIR / f"nifty_xgb_forward_{prev_str}.csv"

# --- Load DataFrames ---
actual_df = pd.read_csv(back_file, parse_dates=['Datetime'])
pred_df   = pd.read_csv(fwd_file,  parse_dates=['Datetime'])

# --- Prepare for merge ---
# Rename forward price so we keep actual price separate
pred_df = pred_df.rename(columns={
    'Close_Price':    'close_price_pred',
    'Predicted_Price':'predicted_price'
})
# Keep only needed cols from actual
actual_df = actual_df.rename(columns={
    'Close_Price':   'close_price_act',
    'True_Direction':'true_direction'
})[['Datetime','close_price_act','true_direction']]

# --- Merge on the 5‑min timestamp ---
cmp = pd.merge(
    pred_df,
    actual_df,
    on='Datetime',
    how='inner'
)

# --- Compute correctness and error magnitude ---
cmp['was_correct'] = cmp['Prediction'] == cmp['true_direction']
cmp['error_mag']   = np.abs(cmp['predicted_price'] - cmp['close_price_act'])

# --- Save merged raw comparison ---
comparison_file = OUTPUT_DIR / f"predictions_comparison_{today_str}.csv"
cmp.to_csv(comparison_file, index=False)
print(f"✅ Merged comparison saved to: {comparison_file}")

# --- Daily summary aggregates ---
total     = len(cmp)
correct   = cmp['was_correct'].sum()
incorrect = total - correct
accuracy  = correct / total * 100 if total else np.nan
avg_error = cmp.loc[~cmp['was_correct'], 'error_mag'].mean() if incorrect else 0.0

summary = pd.DataFrame([{
    'date':           today.date(),
    'total_bars':     total,
    'correct':        correct,
    'incorrect':      incorrect,
    'accuracy_pct':   round(accuracy, 2),
    'avg_error_mag':  round(avg_error, 4)
}])

summary_file = OUTPUT_DIR / f"daily_summary_{today_str}.csv"
summary.to_csv(summary_file, index=False)
print(f"Daily summary saved to:    {summary_file}")
print(summary.to_string(index=False))
