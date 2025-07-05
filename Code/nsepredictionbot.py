# nse_forward_xgb.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from pathlib import Path
import joblib  # for loading the xgb model

# --- Config ---
MODEL_FILE = Path("./models/xgb_nifty_backward.pkl")   # <-- your saved XGB model
INPUT_FILE = Path("./Output") / f"nifty_xgb_backward_{(datetime.now()-timedelta(days=1)).strftime('%Y%m%d')}.csv"
OUTPUT_DIR = Path("./Output")
OUTPUT_DIR.mkdir(exist_ok=True)

# 1) Load the trained XGBoost model
model = joblib.load(MODEL_FILE)

# 2) Read the backward results CSV to get the last known features
df = pd.read_csv(INPUT_FILE, parse_dates=['Datetime'])
latest = df.iloc[-1]
last_close  = latest['Close']
last_sma5   = latest['SMA_5']
last_sma20  = latest['SMA_20']
last_rsi    = latest['RSI']
last_atr    = latest['ATR']

# 3) Compute next trading day (skip Sat/Sun)
today = datetime.now()
next_day = today + timedelta(days=1)
while next_day.weekday() >= 5:
    next_day += timedelta(days=1)

# 4) Generate 5m bar timestamps for next day
market_times = pd.date_range(
    start=datetime.combine(next_day.date(), time(9, 15)),
    end  =datetime.combine(next_day.date(), time(15, 30)),
    freq='5min'
)

# 5) Simulate forward prediction
preds = []
for ts in market_times:
    X = np.array([[last_sma5, last_sma20, last_rsi, last_atr, last_close]])
    direction = int(model.predict(X)[0])
    pct_change = 0.005  # you can refine this to a learned or fixed value
    
    # expected price move
    pred_price = last_close * (1 + pct_change) if direction==1 else last_close * (1 - pct_change)

    preds.append({
        'Datetime':     ts,
        'Close_Price':  last_close,
        'Prediction':   direction,
        'Predicted_Price': pred_price
    })

# 6) Save forward predictions
out_df = pd.DataFrame(preds)
out_file = OUTPUT_DIR / f"nifty_xgb_forward_{next_day.strftime('%Y%m%d')}.csv"
out_df.to_csv(out_file, index=False)

print(f"✅ Saved {len(out_df)} XGB forward predictions for {next_day.date()} to {out_file}")
print(out_df.head())
