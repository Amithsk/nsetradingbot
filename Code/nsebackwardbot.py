# nse_backward_xgb.py
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.linear_model    import LogisticRegression
from sklearn.ensemble        import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import xgboost as xgb
import lightgbm as lgb
import joblib

# 1) Parameters

#Date stuff
INTERVAL = '5m'
today = datetime.now()
# roll back weekends
if today.weekday() == 5: today -= timedelta(days=1)
elif today.weekday() == 6: today -= timedelta(days=2)
end_date = today

start_date = end_date - timedelta(days=55)
start_str = start_date.strftime('%Y-%m-%d')
end_str   = end_date.strftime('%Y-%m-%d')

#Currently yfiance provides today-1 data,so N-1 today date information
file_date = (today - timedelta(days=1))
if file_date.weekday() == 0:  # Monday
    file_date = (today - timedelta(days=3)).strftime('%Y%m%d')
else:
    file_date = (today - timedelta(days=1)).strftime('%Y%m%d')


#Folders for the ouput
folder_date =end_date.strftime('%Y%m%d')
OUTPUT_DIR = Path(f"./Output/{folder_date}/backward")
MODEL_DIR = Path("./models")
OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
MODEL_DIR.mkdir(parents=True,exist_ok=True)

#Data frames
results=[]

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

#6) Model training
models = {
    'Logistic': LogisticRegression(
        max_iter=500,
        solver='lbfgs',     # default good for binary classification
        C=1.0               # inverse of regularization strength
    ),
    'RF': RandomForestClassifier(
        n_estimators=100,
        max_depth=6,
        random_state=42,
        n_jobs=-1
    ),
    'XGB': xgb.XGBClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        use_label_encoder=False,
        eval_metric='logloss',
        random_state=42
    ),
    'LGBM': lgb.LGBMClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
}

for name, model in models.items():
    print(f"Training {name}...")
    model.fit(X_train, y_train)
    
    # 7) Predict & evaluate
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    # Extract full classification report as dictionary
    report_dict = classification_report(y_test, preds, output_dict=True)
    # Flatten key metrics for CSV summary
    summary_row = {
        'model': name,
        'accuracy': acc,
        'precision_0': report_dict['0']['precision'],
        'recall_0': report_dict['0']['recall'],
        'f1_0': report_dict['0']['f1-score'],
        'precision_1': report_dict['1']['precision'],
        'recall_1': report_dict['1']['recall'],
        'f1_1': report_dict['1']['f1-score'],
        'macro_f1': report_dict['macro avg']['f1-score']
        }

    # Append to a growing list
    results.append(summary_row)

    # 8) Build results DataFrame
    res = nifty.iloc[-len(y_test):].copy()
    res['Prediction'] = preds
    res['Was_Correct'] = np.where(res['Prediction']==res['Target'], 'Success','Fail')
    res['Predicted_Price'] = np.where(res['Prediction']==1,res['Close']*(1+res['Pct_Change'].abs()/100),res['Close']*(1-res['Pct_Change'].abs()/100))
    out = res.reset_index()[[
    'Datetime','Close','Predicted_Price','Target','Prediction',
    'Was_Correct','Pct_Change','SMA_5','SMA_20','RSI','ATR'
        ]]
    # 9) Save

    #Model output
    out.to_csv(OUTPUT_DIR/f"nifty_{name}_backward_{file_date}.csv", index=False)
    print(f"Saved backward {name} results to: {OUTPUT_DIR}/nifty_{name}_backward_{file_date}.csv")
    
    #Model details
    joblib.dump(model,f"./models/{name}_backward.pkl")
    
# Accuracy & Classification details
metrics_df = pd.DataFrame(results)
metrics_df.to_csv(OUTPUT_DIR / f"model_metrics_{file_date}.csv", index=False)
print("Saved classification summary for all models.")

# Save folder to file so GitHub Actions can access it
with open("folder_date.txt", "w") as f:
    print("The values are",folder_date,file_date)
    f.write(f"{folder_date},{file_date}")

