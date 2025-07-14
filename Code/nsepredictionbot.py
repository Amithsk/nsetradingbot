# nse_forward_multi.py
import pandas as pd, numpy as np
from datetime import datetime, timedelta, time
from pathlib import Path
import joblib

# 1) Parameters
#Date stuff
today = datetime.now()
# compute next trading day…
next_day = today + timedelta(days=1)
while next_day.weekday() >= 5:  next_day += timedelta(days=1)
tstr = next_day.strftime('%Y%m%d')
folder_date=today.strftime('%Y%m%d')

# Discover all backward CSVs & models
MODEL_DIR  = Path('./models')
OUTPUT_BACK_DIR = Path(f"./Output/{folder_date}/backward")
OUTPUT_FORWARD_DIR=Path(f"./Output/{tstr}/forward")
OUTPUT_FORWARD_DIR.mkdir(parents=True,exist_ok=True)
#Data frame
summary_list = []



for model_path in MODEL_DIR.glob("*_backward.pkl"):
    name = model_path.stem.replace("_backward","")  
    # 2) load
    m = joblib.load(model_path)

    # 3) read its own backward CSV for last features
    back_csv = OUTPUT_BACK_DIR/f"nifty_{name}_{folder_date}.csv"
    df = pd.read_csv(back_csv, parse_dates=['Datetime'])
    latest = df.iloc[-1]
    last_close, last_sma5, last_sma20, last_rsi, last_atr = (
       latest['Close'], latest['SMA_5'], latest['SMA_20'], latest['RSI'], latest['ATR']
    )

    # 4) generate 5m bars for next_day
    times = pd.date_range(
      start=datetime.combine(next_day.date(), time(9,15)),
      end  =datetime.combine(next_day.date(), time(15,30)),
      freq='5min'
    )
    records = []
    for ts in times:
        X = np.array([[ last_sma5, last_sma20, last_rsi, last_atr, last_close ]])
        direction = int(m.predict(X)[0])
        pct       = pct = abs(latest['Pct_Change']) / 100  # or a model‐specific learned magnitude
        pred_price= last_close*(1+pct) if direction else last_close*(1-pct)

        records.append({
          'Datetime': ts,
          'Close_Price': last_close,
          'Prediction': direction,
          'Predicted_Price': pred_price
        })
    pred_df = pd.DataFrame(records)
    pred_df.to_csv(OUTPUT_FORWARD_DIR/f"nifty_{name}_forward_{tstr}.csv", index=False)
    print(f"Saved forward for {name} to: nifty_{name}_forward_{tstr}.csv")
    

    # Summarize predictions
    num_total   = len(pred_df)
    num_bullish = pred_df['Prediction'].sum()
    num_bearish = num_total - num_bullish
    bullish_pct = num_bullish / num_total * 100

    summary_data = {
    'model': name,
    'date':  tstr,
    'total_bars': num_total,
    'bullish': num_bullish,
    'bearish': num_bearish,
    'bullish_pct': round(bullish_pct, 2),
    'predicted_close_mean': round(pred_df['Predicted_Price'].mean(), 2)
    }

    # Append to a list (define `summary_list = []` before the loop)
    summary_list.append(summary_data)

# Save forward summary for all models
summary_df = pd.DataFrame(summary_list)
summary_file = OUTPUT_FORWARD_DIR / f"forward_summary_{tstr}.csv"
summary_df.to_csv(summary_file, index=False)
print(f"Saved forward summary: {summary_file}")