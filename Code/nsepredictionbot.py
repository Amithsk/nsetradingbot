import yfinance as yf
from datetime import datetime, timedelta
import gymnasium as gym
import numpy as np
import pandas as pd
from pathlib import Path
from stable_baselines3 import DQN

# --- Configuration ---
MODEL_PATH = "./Output/dqn_nifty_final.zip"
OUTPUT_DIR = Path("./Output")
OUTPUT_DIR.mkdir(exist_ok=True)

today = datetime.now()
today_str = today.strftime('%Y%m%d')

# Use last 55 days of data including today
start_date = (today - timedelta(days=55)).strftime('%Y-%m-%d')
end_date = today.strftime('%Y-%m-%d')

# --- Download daily data for prediction ---
nifty = yf.download('^NSEI', start=start_date, end=end_date, interval='1d')
nifty.columns = [col[0] if isinstance(col, tuple) else col for col in nifty.columns]
nifty.reset_index(inplace=True)
nifty.dropna(inplace=True)

# Add indicators
def compute_rsi(series, window=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    rs = avg_gain / (avg_loss + 1e-8)
    return 100 - (100 / (1 + rs))

def compute_atr(df, window=14):
    high_low = df['High'] - df['Low']
    high_close = np.abs(df['High'] - df['Close'].shift())
    low_close = np.abs(df['Low'] - df['Close'].shift())
    tr = np.maximum(high_low, np.maximum(high_close, low_close))
    return tr.rolling(window).mean()

nifty['SMA_5'] = nifty['Close'].rolling(5).mean()
nifty['SMA_20'] = nifty['Close'].rolling(20).mean()
nifty['RSI'] = compute_rsi(nifty['Close'], window=14)
nifty['ATR'] = compute_atr(nifty)
nifty.dropna(inplace=True)

# --- Define environment (same as training) ---
class TradingEnv(gym.Env):
    def __init__(self, df):
        super().__init__()
        self.df = df.reset_index(drop=True)
        self.current_step = len(self.df) - 1  # Only predict the last row
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(low=-np.inf, high=np.inf, shape=(5,), dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        obs = self.df.loc[self.current_step, ['Close', 'SMA_5', 'SMA_20', 'RSI', 'ATR']].values
        return obs.astype(np.float32), {}

    def step(self, action):
        return np.zeros(self.observation_space.shape), 0.0, True, False, {}

# --- Run prediction ---
env = TradingEnv(nifty)
model = DQN.load(MODEL_PATH)
obs, _ = env.reset()
action, _ = model.predict(obs, deterministic=True)
predicted_direction = int(action)

# --- Save prediction ---
latest_row = nifty.iloc[-1]
predicted_price = (
    latest_row['Close'] * (1 + 0.01) if predicted_direction == 1
    else latest_row['Close'] * (1 - 0.01)
)

result = pd.DataFrame([{
    'Date': latest_row['Date'],
    'Close_Price': latest_row['Close'],
    'Prediction': predicted_direction,
    'Predicted_Price': predicted_price
}])

result.to_csv(OUTPUT_DIR / f"nifty_rl_predict_{today_str}.csv", index=False)
print(f"✅ Prediction saved to: ./Output/nifty_rl_predict_{today_str}.csv")
print(result)
