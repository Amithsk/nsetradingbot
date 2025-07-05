# nse_backward_xgb.py
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import xgboost as xgb
import joblib

# 1) Parameters
INTERVAL = '5m'
today = datetime.now()
# roll back weekends
if today.weekday() == 5: today -= timedelta(days=1)
elif today.weekday() == 6: today -= timedelta(days=2)
end_date = today
start_date = end_date - timedelta(days=55)
start_str = start_date.strftime('%Y-%m-%d')
end_str   = end_date.strftime('%Y-%m-%d')

# 2) Download data
nifty = yf.download('^NSEI', start=start_str, end=end_str, interval=INTERVAL)
nifty.columns = [c[0] for c in nifty.columns]
nifty = nifty.reset_index().dropna()

# 3) Feature engineering
def compute_rsi(s, window=14):
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    rs = avg_gain / (avg_loss + 1e-8)
    return 100 - (100/(1+rs))

def compute_atr(df, window=14):
    hl = df['High'] - df['Low']
    hc = (df['High'] - df['Close'].shift()).abs()
    lc = (df['Low']  - df['Close'].shift()).abs()
    tr = np.maximum(hl, np.maximum(hc, lc))
    return tr.rolling(window).mean()

nifty['SMA_5']  = nifty['Close'].rolling(5).mean()
nifty['SMA_20'] = nifty['Close'].rolling(20).mean()
nifty['RSI']    = compute_rsi(nifty['Close'])
nifty['ATR']    = compute_atr(nifty)
nifty.dropna(inplace=True)

# 4) Target and features
nifty['Target'] = (nifty['Close'].shift(-1) > nifty['Close']).astype(int)
nifty['Pct_Change'] = (nifty['Close'].shift(-1)-nifty['Close'])/nifty['Close']*100
nifty.dropna(inplace=True)

FEATURES = ['SMA_5','SMA_20','RSI','ATR','Close']
X = nifty[FEATURES]
y = nifty['Target']

# 5) Train/test split (chronological)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, shuffle=False, test_size=0.2
)

# 6) Fit XGBoost
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=4,
    learning_rate=0.1,
    use_label_encoder=False,
    eval_metric='logloss'
)
model.fit(X_train, y_train)

# 7) Predict & evaluate
preds = model.predict(X_test)
acc = accuracy_score(y_test, preds)
print("Test Accuracy:", acc)
print(classification_report(y_test, preds))

# 8) Build results DataFrame
res = nifty.iloc[-len(y_test):].copy()
res['Prediction'] = preds
res['Was_Correct'] = np.where(res['Prediction']==res['Target'], 'Success','Fail')
res['Predicted_Price'] = np.where(
    res['Prediction']==1,
    res['Close']*(1+res['Pct_Change'].abs()/100),
    res['Close']*(1-res['Pct_Change'].abs()/100)
)
out = res.reset_index()[[
    'Datetime','Close','Predicted_Price','Target','Prediction',
    'Was_Correct','Pct_Change','SMA_5','SMA_20','RSI','ATR'
]]
# 9) Save
OUTPUT_DIR = Path("./Output")
OUTPUT_DIR.mkdir(exist_ok=True)
today_str = today.strftime('%Y%m%d')
out.to_csv(OUTPUT_DIR/f"nifty_xgb_backward_{today_str}.csv", index=False)
print(f"Saved backward XGB result to: {OUTPUT_DIR}/nifty_xgb_backward_{today_str}.csv")

Path("./models").mkdir(exist_ok=True)
joblib.dump(model, './models/xgb_nifty_backward.pkl')
print("XGBoost model saved to ./models/xgb_nifty_backward.pkl")
