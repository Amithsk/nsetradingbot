import pandas as pd
import numpy as np
import gym
from datetime import datetime, timedelta, time
from stable_baselines3 import DQN
from pathlib import Path

# --- Config ---
MODEL_PATH = "./models/dqn_nifty_final.zip"
OUTPUT_DIR = Path("./Output")
OUTPUT_DIR.mkdir(exist_ok=True)

today = datetime.now()
today_str = today.strftime('%Y%m%d')

# --- Load the latest evaluation CSV ---
eval_file = OUTPUT_DIR / f"nifty_rl_evaluation_{today_str}.csv"
df = pd.read_csv(eval_file, parse_dates=['Date'])

# --- Function: Next Trading Day (Skip weekends) ---
def next_trading_day(current_date):
    next_day = current_date + timedelta(days=1)
    while next_day.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        next_day += timedelta(days=1)
    return next_day

# --- Calculate Next Trading Day ---
next_day = next_trading_day(today)

# NSE trading hours: 09:15 to 15:30 (IST) at 5-minute intervals
market_times = pd.date_range(
    start=datetime.combine(next_day.date(), time(9, 15)),
    end=datetime.combine(next_day.date(), time(15, 30)),
    freq='5min'
)

# --- Prepare last known feature values from evaluation CSV ---
history = df.copy()
model = DQN.load(MODEL_PATH, device="cuda")
print("✅ Model device:", next(model.q_net.parameters()).device)

try:
    last_close = history['Close_Price'].iloc[-1]
    last_rsi = history['RSI'].iloc[-1]
    last_sma5 = history['SMA_5'].iloc[-1]
    last_sma20 = history['SMA_20'].iloc[-1]
    last_atr = history['ATR'].iloc[-1]
except KeyError as e:
    raise KeyError(f"Missing column in evaluation data: {e}. Please ensure RSI, SMA_5, SMA_20, ATR are included.")

# --- Dummy Env for Prediction ---
class PredictEnv(gym.Env):
    def __init__(self, obs):
        super().__init__()
        self.obs = obs
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(low=-np.inf, high=np.inf, shape=(5,), dtype=np.float32)

    def reset(self):
        return self.obs.astype(np.float32)

    def step(self, action):
        return self.obs, 0.0, True, False, {}

# --- Simulate prediction across 5-min bars ---
predictions = []
for dt in market_times:
    obs = np.array([last_close, last_sma5, last_sma20, last_rsi, last_atr])
    env = PredictEnv(obs)
    obs = env.reset()
    action, _ = model.predict(obs, deterministic=True)
    direction = int(action)

    predicted_price = (
        last_close * (1 + 0.005) if direction == 1
        else last_close * (1 - 0.005)
    )

    predictions.append({
        'Datetime': dt,
        'Close_Price': last_close,
        'Prediction': direction,
        'Predicted_Price': predicted_price
    })

# --- Save to CSV ---
pred_df = pd.DataFrame(predictions)
out_path = OUTPUT_DIR / f"nifty_rl_predict_{next_day.strftime('%Y%m%d')}.csv"
pred_df.to_csv(out_path, index=False)

print(f"\n✅ Saved {len(pred_df)} predictions for next trading day ({next_day.date()}) to: {out_path}")
print(pred_df.head())
