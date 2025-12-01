"""
estimate_expected_hold_days helper

Provides estimate_expected_hold_days(...) which returns (expected_days:int, method:str).
It implements the empirical median-days method (primary), ATR-based heuristic (fallback),
score-based heuristic (fallback), and a final default fallback.

Usage:
    from Code.utils.nseintraday_estimate_expected_hold_days import estimate_expected_hold_days

    days, method = estimate_expected_hold_days(engine=engine, symbol='RELIANCE', strategy='momentum', score=2.3, atr_value=0.8)

Notes:
- If you pass engine, the empirical method will query intraday_bhavcopy through SQL.
- You can pass hist_df (pandas DataFrame) instead of engine for unit testing.
"""

import math
from typing import Optional, Tuple, Dict
import pandas as pd
import numpy as np
from sqlalchemy import text

# Small per-process cache to avoid heavy repeated scans
_EXPECTED_HOLD_CACHE: Dict[str, Tuple[int, str]] = {}

def _load_history_for_symbol(engine, symbol: str, lookback_days: int, extra_days: int = 30) -> pd.DataFrame:
    """
    Load daily OHLC rows for the given symbol from intraday_bhavcopy.
    Returns a DataFrame sorted by trade_date ascending (old -> new).
    """
    limit = int(lookback_days + extra_days + 10)
    q = text("""
        SELECT trade_date, open, high, low, close
        FROM intraday_bhavcopy
        WHERE symbol = :sym
        ORDER BY trade_date DESC
        LIMIT :limit
    """)
    df = pd.read_sql(q, engine, params={"sym": symbol, "limit": limit})
    if df.empty:
        return df
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()
    df = df.sort_values('trade_date').reset_index(drop=True)
    return df

def _median_days_to_hit_target_or_stop(
    engine=None,
    symbol: Optional[str] = None,
    hist_df: Optional[pd.DataFrame] = None,
    lookback_days: int = 252,
    pct_stop: float = 0.005,
    pct_target: float = 0.01,
    max_search_days: int = 30,
    min_samples: int = 8
) -> Optional[int]:
    """
    Empirical estimate:
      For each historical row i in the lookback window (treated as 'entry' at close),
      search forward up to `max_search_days` and record days until either:
        - high >= entry*(1+pct_target)  -> target hit
        - low  <= entry*(1-pct_stop)    -> stop hit
      Return median days across successful (uncensored) samples.
    """
    if hist_df is None:
        if engine is None or symbol is None:
            return None
        df = _load_history_for_symbol(engine, symbol, lookback_days, extra_days=max_search_days)
    else:
        df = hist_df.copy()
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()
        df = df.sort_values('trade_date').reset_index(drop=True)

    if df.empty or len(df) < 10:
        return None

    nrows = len(df)
    # restrict to most recent lookback window plus buffer for forward search
    if lookback_days and nrows > lookback_days + max_search_days:
        df = df.iloc[-(lookback_days + max_search_days):].reset_index(drop=True)
        nrows = len(df)

    days_to_exit = []
    for i in range(0, nrows - 1):
        entry = df.loc[i, 'close']
        if pd.isna(entry) or entry <= 0:
            continue
        target_up = entry * (1.0 + pct_target)
        stop_down = entry * (1.0 - pct_stop)
        for j in range(i+1, min(i+1+max_search_days, nrows)):
            high_j = df.loc[j, 'high']
            low_j = df.loc[j, 'low']
            if pd.notna(high_j) and high_j >= target_up:
                days_to_exit.append(j - i)
                break
            if pd.notna(low_j) and low_j <= stop_down:
                days_to_exit.append(j - i)
                break
        # censored (no hit within window) samples are ignored

    if len(days_to_exit) < min_samples:
        return None

    median_days = int(math.ceil(float(np.median(days_to_exit))))
    median_days = max(1, min(30, median_days))
    return median_days

def _atr_based_expected_hold(
    atr_value: Optional[float],
    atr_target_mult: float = 3.0,
    avg_daily_move_factor: float = 1.0,
    min_days: int = 1,
    max_days: int = 30
) -> Optional[int]:
    """
    ATR heuristic: days = ceil( target_move / avg_daily_move )
    where target_move = atr_target_mult * atr_value and avg_daily_move ≈ atr_value * avg_daily_move_factor.
    """
    try:
        if atr_value is None or math.isnan(atr_value) or atr_value <= 0:
            return None
    except Exception:
        return None

    target_move = atr_target_mult * float(atr_value)
    avg_daily_move = max(1e-6, float(atr_value) * float(avg_daily_move_factor))
    est_days = int(math.ceil(target_move / avg_daily_move))
    est_days = max(min_days, min(max_days, est_days))
    return est_days

def estimate_expected_hold_days(
    engine=None,
    symbol: Optional[str] = None,
    strategy: Optional[str] = None,
    score: Optional[float] = None,
    hist_df: Optional[pd.DataFrame] = None,
    atr_value: Optional[float] = None,
    # tuning parameters (defaults ok for most use)
    lookback_days: int = 252,
    pct_stop: float = 0.005,
    pct_target: float = 0.01,
    max_search_days: int = 30,
    min_samples: int = 8,
    atr_target_mult: float = 3.0,
    avg_daily_move_factor: float = 1.0,
    fallback_default: int = 7,
    min_days: int = 1,
    max_days: int = 30,
    use_cache: bool = True
) -> Tuple[int, str]:
    """
    Public wrapper that returns (expected_days, method).
    Methods: "empirical", "atr", "score", "fallback".
    """
    cache_key = f"{symbol}|{strategy}|{lookback_days}|{pct_stop}|{pct_target}|{max_search_days}"
    if use_cache and cache_key in _EXPECTED_HOLD_CACHE:
        return _EXPECTED_HOLD_CACHE[cache_key]

    # 1) empirical
    emp = _median_days_to_hit_target_or_stop(
        engine=engine,
        symbol=symbol,
        hist_df=hist_df,
        lookback_days=lookback_days,
        pct_stop=pct_stop,
        pct_target=pct_target,
        max_search_days=max_search_days,
        min_samples=min_samples
    )
    if emp is not None:
        method = "empirical"
        res = (int(emp), method)
        if use_cache:
            _EXPECTED_HOLD_CACHE[cache_key] = res
        return res

    # 2) ATR-based
    if atr_value is not None:
        atr_est = _atr_based_expected_hold(
            atr_value=atr_value,
            atr_target_mult=atr_target_mult,
            avg_daily_move_factor=avg_daily_move_factor,
            min_days=min_days,
            max_days=max_days
        )
        if atr_est is not None:
            method = "atr"
            res = (int(atr_est), method)
            if use_cache:
                _EXPECTED_HOLD_CACHE[cache_key] = res
            return res

    # 3) score-based simple heuristic
    if score is not None:
        try:
            s = float(score)
            shrink = min(0.6, abs(s) / 10.0)
            base = fallback_default
            est_days = max(min_days, int(round(base * (1.0 - shrink))))
            est_days = min(max_days, est_days)
            method = "score"
            res = (int(est_days), method)
            if use_cache:
                _EXPECTED_HOLD_CACHE[cache_key] = res
            return res
        except Exception:
            pass

    # 4) final fallback
    method = "fallback"
    final = max(min_days, min(max_days, int(fallback_default)))
    res = (final, method)
    if use_cache:
        _EXPECTED_HOLD_CACHE[cache_key] = res
    return res

def clear_expected_hold_cache():
    """Clear the per-process in-memory cache."""
    _EXPECTED_HOLD_CACHE.clear()

# -----------------------
# Small self-check / unit test routine (runs only when executed directly)
# -----------------------
if __name__ == "__main__":
    # synthetic quick test (no DB required)
    dates = pd.date_range(end=pd.Timestamp("2025-11-01"), periods=40, freq='B')
    closes = np.linspace(100, 120, len(dates)) + np.random.normal(scale=0.5, size=len(dates))
    highs = closes * (1 + 0.02 * np.abs(np.sin(np.linspace(0, 6.28, len(dates)))))
    lows = closes * (1 - 0.01 * np.abs(np.sin(np.linspace(0, 6.28, len(dates)))))
    hist = pd.DataFrame({"trade_date": dates, "open": closes * 0.995, "high": highs, "low": lows, "close": closes})
    med = _median_days_to_hit_target_or_stop(hist_df=hist, lookback_days=30, pct_stop=0.005, pct_target=0.01, max_search_days=10, min_samples=3)
    print("Empirical median (synthetic):", med)
    atr_val = 0.8
    atr_est = _atr_based_expected_hold(atr_val, atr_target_mult=3.0, avg_daily_move_factor=1.0)
    print("ATR-based estimate (atr=0.8):", atr_est)
    days, method = estimate_expected_hold_days(hist_df=hist, symbol="SYN", strategy="momentum", atr_value=atr_val, score=2.5)
    print("estimate_expected_hold_days =>", days, method)
