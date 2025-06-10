import yfinance as yf
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import accuracy_score, classification_report

# --- Dynamically determine workspace root ---
workspace_root = Path(__file__).resolve().parent.parent
# Join with Output directory
output_dir = workspace_root / "Output"
output_dir.mkdir(exist_ok=True)

# Download NIFTY50 daily data
nifty = yf.download('^NSEI', start='2020-01-01', end='2024-12-31', interval='1d')
nifty.dropna(inplace=True)


# Flatten the MultiIndex
nifty.columns = [col[0] for col in nifty.columns]




# Technical indicators
# Moving Averages
nifty['SMA_5'] = nifty['Close'].rolling(window=5).mean()
nifty['SMA_20'] = nifty['Close'].rolling(window=20).mean()
# Bollinger Bands
nifty['BB_MID'] = nifty['Close'].rolling(window=20).mean()
nifty['BB_UPPER'] = nifty['BB_MID'] + 2 * nifty['Close'].rolling(window=20).std()
nifty['BB_LOWER'] = nifty['BB_MID'] - 2 * nifty['Close'].rolling(window=20).std()
nifty['VWAP'] = (nifty['High'] + nifty['Low'] + nifty['Close']) / 3
nifty['Pivot'] = (nifty['High'].shift(1) + nifty['Low'].shift(1) + nifty['Close'].shift(1)) / 3
nifty['Support1'] = (2 * nifty['Pivot']) - nifty['High'].shift(1)
nifty['Resistance1'] = (2 * nifty['Pivot']) - nifty['Low'].shift(1)

# Target: 1 for bullish next day, 0 for bearish
nifty['Target'] = np.where(nifty['Close'].shift(-1) > nifty['Close'], 1, 0)
nifty['Pct_Change'] = (nifty['Close'].shift(-1) - nifty['Close']) / nifty['Close'] * 100

nifty.dropna(inplace=True)

# Feature set
features = ['SMA_5', 'SMA_20', 'BB_UPPER', 'BB_LOWER', 'VWAP', 'Pivot', 'Support1', 'Resistance1']
X = nifty[features]
y = nifty['Target']

# Train/Test split
X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=False, test_size=0.2)

# MLP Classifier
clf = MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=500, random_state=42)
clf.fit(X_train, y_train)
preds = clf.predict(X_test)

# Result dataframe
results = nifty.iloc[-len(y_test):].copy()
results['Prediction'] = preds
results['Prediction_Result'] = np.where(results['Prediction'] == results['Target'], 'Success', 'Fail')

# Save results
results_to_save = results[['Close', 'Prediction', 'Target', 'Prediction_Result', 'Pct_Change']]
results_to_save.reset_index(inplace=True)
results_to_save.to_csv(output_dir / 'nifty_prediction_results.csv', index=False)
print(f"✅ Results saved to: {output_dir / 'nifty_prediction_results.csv'}")

# Print metrics
print("Accuracy:", accuracy_score(y_test, preds))
print("Classification Report:\n", classification_report(y_test, preds))
