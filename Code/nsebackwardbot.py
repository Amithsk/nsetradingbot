import yfinance as yf
import gym
import numpy as np
from pathlib import Path
from stable_baselines3 import DQN
from stable_baselines3.common.vec_env import DummyVecEnv
from stable_baselines3.common.callbacks import CheckpointCallback
from datetime import datetime, timedelta
import pandas as pd

# --- Parameters ---
interval_time='5m'
# Get today's date
today = datetime.now()

# Shift if weekend
if today.weekday() == 5:  # Saturday
    today -= timedelta(days=1)
elif today.weekday() == 6:  # Sunday
    today -= timedelta(days=2)

# Limit to max 5-min range (Yahoo supports only ~60 days)
date_end = today
date_start = date_end - timedelta(days=55)  # keep some buffer

# Format for yfinance
date_start = date_start.strftime('%Y-%m-%d')
date_end = date_end.strftime('%Y-%m-%d')

# --- Download data ---
nifty = yf.download('^NSEI', start=date_start, end=date_end, interval=interval_time)

print("The column values", nifty.columns )

# Flatten the multi-level columns
nifty.columns = [col[0] for col in nifty.columns]

# Reset index to get 'Datetime' column
nifty.reset_index(inplace=True)
nifty.dropna(inplace=True)

print("The column values", nifty.columns )


# Ensure 'Datetime' is timezone-aware (IST)
nifty['Datetime'] = nifty['Datetime'].dt.tz_convert('Asia/Kolkata')

# Filter time between 9:15 and 15:30 IST
nifty = nifty[(nifty['Datetime'].dt.time >= pd.to_datetime('09:15').time()) &
              (nifty['Datetime'].dt.time <= pd.to_datetime('15:30').time())]





# --- Feature Engineering ---
def add_technical_indicators(df):
    df['SMA_5'] = df['Close'].rolling(5).mean()
    df['SMA_20'] = df['Close'].rolling(20).mean()
    df['RSI'] = compute_rsi(df['Close'], window=14)
    df['ATR'] = compute_atr(df)
    df.dropna(inplace=True)
    return df

# RSI helper
def compute_rsi(series, window=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    rs = avg_gain / (avg_loss + 1e-8)
    return 100 - (100 / (1 + rs))

# ATR helper
def compute_atr(df, window=14):
    high_low = df['High'] - df['Low']
    high_close = np.abs(df['High'] - df['Close'].shift())
    low_close = np.abs(df['Low'] - df['Close'].shift())
    tr = np.maximum(high_low, np.maximum(high_close, low_close))
    return tr.rolling(window).mean()

nifty = add_technical_indicators(nifty)

# Add true direction and percent change
nifty['True_Direction'] = np.where(nifty['Close'].shift(-1) > nifty['Close'], 1, 0)
nifty['Pct_Change'] = (nifty['Close'].shift(-1) - nifty['Close']) / nifty['Close'] * 100
nifty.dropna(inplace=True)

# --- Gym Environment ---
class TradingEnv(gym.Env):
    metadata = {'render.modes': ['human']}

    def __init__(self, df, initial_balance=1e6):
        super(TradingEnv, self).__init__()
        self.df = df.reset_index(drop=True)
        self.initial_balance = initial_balance
        self.action_space = gym.spaces.Discrete(2)  # 0: Predict bearish, 1: Predict bullish
        # Observations: technical features only
        self.observation_space = gym.spaces.Box(
            low=-np.inf, high=np.inf,
            shape=(len(['Close','SMA_5','SMA_20','RSI','ATR']),),
            dtype=np.float32)
        self.reset()

    def reset(self):
        self.balance = self.initial_balance
        self.current_step = 0
        return self._next_observation()

    def _next_observation(self):
        obs = self.df.loc[self.current_step, ['Close','SMA_5','SMA_20','RSI','ATR']].values
        return obs.astype(np.float32)

    def step(self, action):
        price = self.df.loc[self.current_step, 'Close']
        true_dir = self.df.loc[self.current_step, 'True_Direction']

        # reward simply 1 for correct prediction, else 0
        reward = 1.0 if int(action) == int(true_dir) else 0.0
        
        self.balance += 0  # no PnL tracking here, for classification reward

        # Record result for evaluation
        self.current_step += 1
        done = self.current_step >= len(self.df) - 1
        obs = self._next_observation() if not done else np.zeros(self.observation_space.shape)
        info = {}
        return obs, reward, done, info

    def render(self, mode='human'):
        print(f'Step: {self.current_step}, Balance: {self.balance:.2f}')

# --- Prepare environment ---
env = DummyVecEnv([lambda: TradingEnv(nifty)])

# --- RL Training ---
checkpoint_callback = CheckpointCallback(save_freq=10000, save_path='./models/', name_prefix='dqn_nifty')
model = DQN(
        'MlpPolicy',
          env, 
          verbose=1,
            learning_rate=1e-4,
            buffer_size=50000,
              learning_starts=1000,
                batch_size=32,
            gamma=0.99,
              target_update_interval=500,
              device="cuda")
model.learn(total_timesteps=200000, callback=checkpoint_callback)

Path("./models").mkdir(exist_ok=True)
model.save('./models/dqn_nifty_final')
#print("Training complete. Model saved to 'dqn_nifty_final.zip'!")

# --- Evaluation ---
eval_steps = len(nifty) - 1
predictions = []
true_dirs = []
miss_pct = []
obs = env.reset()
for step in range(eval_steps):
    action, _ = model.predict(obs, deterministic=True)
    predictions.append(int(action[0]))
    true = int(nifty.iloc[step]['True_Direction'])
    true_dirs.append(true)
    # Calculate miss percentage
    if action[0] != true:
        pct = abs(nifty.iloc[step]['Pct_Change'])
    else:
        pct = 0.0
    miss_pct.append(pct)
    obs, _, done, _ = env.step(action)
    if done:
        break

# Compile results
actual_prices = nifty['Close'].values[:len(predictions)]
predicted_prices = []

for step, pred in enumerate(predictions):
    current_price = nifty['Close'].iloc[step]
    pct_change = nifty['Pct_Change'].iloc[step]

    if pred == 1:
        # Bullish prediction: expect price to go up
        predicted_price = current_price * (1 + abs(pct_change) / 100)
    else:
        # Bearish prediction: expect price to go down
        predicted_price = current_price * (1 - abs(pct_change) / 100)
    
    predicted_prices.append(predicted_price)

# Compile results
results = pd.DataFrame({
    'Date': nifty['Datetime'][:len(predictions)],
    'Close_Price': actual_prices,
    'True_Direction': true_dirs, #True_Direction = 1 (bullish)/True_Direction = 0 (bearish/netural)
    'Prediction': predictions,#Prediction = 1 (Bullish)/Prediction = 0 (Bearish)
    'Predicted_Price': predicted_prices,
    'Was_Correct': ['Success' if p==t else 'Fail' for p,t in zip(predictions, true_dirs)],
    'Miss_Percentage': miss_pct,
    'Pct_Change': nifty['Pct_Change'].values[:len(predictions)],
    'RSI': nifty['RSI'].values[:len(predictions)],
    'SMA_5': nifty['SMA_5'].values[:len(predictions)],
    'SMA_20': nifty['SMA_20'].values[:len(predictions)],
    'ATR': nifty['ATR'].values[:len(predictions)]
    
})


# Save to CSV
Path("./Output").mkdir(exist_ok=True)
results.to_csv("./Output/nifty_rl_evaluation.csv", index=False)
print("Evaluation complete. Results saved to './Output/nifty_rl_evaluation.csv'")


