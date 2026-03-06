# nse_backward_xgb.py
# Download 5m data for last 50 days, train backward-looking models, and save results.

import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import xgboost as xgb
import lightgbm as lgb
import joblib
import requests
import time, random
from utils.nseholiday import nseholiday
from zoneinfo import ZoneInfo

# -------------------------------
# 1) Parameters
# -------------------------------

INTERVAL = "5m"

print("----- NSE BACKWARD PIPELINE START -----")

# Ensure IST timezone (important for GitHub runners)
now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
print(f"Pipeline run time (IST): {now_ist}")

today = now_ist

# Weekend protection
if today.weekday() == 5:
    print("Today is Saturday → rolling back to Friday")
    today -= timedelta(days=1)

elif today.weekday() == 6:
    print("Today is Sunday → rolling back to Friday")
    today -= timedelta(days=2)

end_date = today
start_date = end_date - timedelta(days=55)

start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

print(f"Data download window: {start_str} → {end_str}")

# -------------------------------
# Folder setup (temporary)
# -------------------------------

MODEL_DIR = Path("./models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

results = []

# -------------------------------
# 2) Download data (bulk + Chart API patch)
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

    print(f"Found {len(missing_times)} missing intervals → attempting patch...")

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
# 3) Data Fetch + Validation
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
        print(f"{last_candle_date} is an NSE holiday → skipping pipeline.")
        exit(0)

    # Accept data immediately
    print("Yahoo data accepted.")
    break

else:

    raise RuntimeError("Yahoo data fetch failed after retries.")


# -------------------------------
# Determine output dates
# -------------------------------

file_date = last_candle_date.strftime("%Y%m%d")
folder_date = file_date

print(f"File date (market session): {file_date}")
print(f"Output folder date: {folder_date}")

OUTPUT_DIR = Path(f"./Output/{folder_date}/backward")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Continue processing
# -------------------------------

nifty = nifty.reset_index().dropna()

print(f"Total rows downloaded: {len(nifty)}")

# -------------------------------
# Feature engineering
# -------------------------------

def compute_rsi(s, window=14):

    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()

    rs = avg_gain / (avg_loss + 1e-8)

    return 100 - (100/(1+rs))


def compute_atr(df, window=14):

    hl = df["High"] - df["Low"]
    hc = (df["High"] - df["Close"].shift()).abs()
    lc = (df["Low"] - df["Close"].shift()).abs()

    tr = np.maximum(hl, np.maximum(hc, lc))

    return tr.rolling(window).mean()


nifty["SMA_5"]  = nifty["Close"].rolling(5).mean()
nifty["SMA_20"] = nifty["Close"].rolling(20).mean()
nifty["RSI"]    = compute_rsi(nifty["Close"])
nifty["ATR"]    = compute_atr(nifty)

nifty.dropna(inplace=True)

print("Feature engineering completed.")

# -------------------------------
# Target
# -------------------------------

nifty["Target"] = (nifty["Close"].shift(-1) > nifty["Close"]).astype(int)

nifty["Pct_Change"] = (
    (nifty["Close"].shift(-1) - nifty["Close"]) / nifty["Close"] * 100
)

nifty.dropna(inplace=True)

FEATURES = ["SMA_5","SMA_20","RSI","ATR","Close"]

X = nifty[FEATURES]
y = nifty["Target"]

# -------------------------------
# Train/test split
# -------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, shuffle=False, test_size=0.2
)

print(f"Train size: {len(X_train)} | Test size: {len(X_test)}")

# -------------------------------
# Models
# -------------------------------

models = {

"Logistic": LogisticRegression(max_iter=500, solver="lbfgs", C=1.0),

"RF": RandomForestClassifier(
    n_estimators=100,
    max_depth=6,
    random_state=42,
    n_jobs=-1
),

"XGB": xgb.XGBClassifier(
    n_estimators=100,
    max_depth=4,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    use_label_encoder=False,
    eval_metric="logloss",
    random_state=42
),

"LGBM": lgb.LGBMClassifier(
    n_estimators=100,
    max_depth=4,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)

}

# -------------------------------
# Train + Save
# -------------------------------

for name, model in models.items():

    print(f"\nTraining model: {name}")

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    acc = accuracy_score(y_test, preds)

    report_dict = classification_report(y_test, preds, output_dict=True)

    summary_row = {
        "model": name,
        "accuracy": acc,
        "precision_0": report_dict["0"]["precision"],
        "recall_0": report_dict["0"]["recall"],
        "f1_0": report_dict["0"]["f1-score"],
        "precision_1": report_dict["1"]["precision"],
        "recall_1": report_dict["1"]["recall"],
        "f1_1": report_dict["1"]["f1-score"],
        "macro_f1": report_dict["macro avg"]["f1-score"],
    }

    results.append(summary_row)

    res = nifty.iloc[-len(y_test):].copy()

    res["Prediction"] = preds

    res["Was_Correct"] = np.where(
        res["Prediction"] == res["Target"],
        "Success",
        "Fail"
    )

    res["Predicted_Price"] = np.where(
        res["Prediction"] == 1,
        res["Close"]*(1+res["Pct_Change"].abs()/100),
        res["Close"]*(1-res["Pct_Change"].abs()/100)
    )

    out = res.reset_index()[[
        "Datetime","Open","High","Low","Volume","Close",
        "Predicted_Price","Target","Prediction","Was_Correct",
        "Pct_Change","SMA_5","SMA_20","RSI","ATR"
    ]]

    out_file = OUTPUT_DIR / f"nifty_{name}_backward_{file_date}.csv"

    out.to_csv(out_file,index=False)

    print(f"Saved: {out_file}")

    joblib.dump(model,f"./models/{name}_backward.pkl")

# -------------------------------
# Save metrics
# -------------------------------

metrics_df = pd.DataFrame(results)

metrics_file = OUTPUT_DIR / f"model_metrics_{file_date}.csv"

metrics_df.to_csv(metrics_file,index=False)

print(f"Metrics saved: {metrics_file}")

# -------------------------------
# Write date for GitHub Action
# -------------------------------

with open("backward_date.txt","w") as f:
    f.write(f"{folder_date},{file_date}")

print("backward_date.txt written for GitHub Actions")

print("----- NSE BACKWARD PIPELINE COMPLETE -----")