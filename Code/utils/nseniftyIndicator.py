# Code/utils/nseinfityindicator.py
# =========================================================
# File: utils/indicators.py
# Purpose: Technical indicator calculations (reusable)
# =========================================================

import pandas as pd
import numpy as np


# ---------------------------------------------------------
# SIMPLE MOVING AVERAGE (SMA)
# ---------------------------------------------------------

def compute_sma(series: pd.Series, window: int) -> pd.Series:
    """
    Compute Simple Moving Average

    Args:
        series: Price series (typically Close)
        window: Rolling window

    Returns:
        pd.Series
    """
    return series.rolling(window=window).mean()


# ---------------------------------------------------------
# RELATIVE STRENGTH INDEX (RSI)
# ---------------------------------------------------------

def compute_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    """
    Compute RSI (Relative Strength Index)

    Args:
        series: Price series (Close)
        window: Lookback period

    Returns:
        pd.Series
    """

    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=window).mean()
    avg_loss = loss.rolling(window=window).mean()

    rs = avg_gain / (avg_loss + 1e-8)

    rsi = 100 - (100 / (1 + rs))

    return rsi


# ---------------------------------------------------------
# AVERAGE TRUE RANGE (ATR)
# ---------------------------------------------------------

def compute_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """
    Compute ATR (Average True Range)

    Args:
        df: DataFrame with High, Low, Close
        window: Lookback period

    Returns:
        pd.Series
    """

    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    hl = high - low
    hc = (high - close.shift()).abs()
    lc = (low - close.shift()).abs()

    tr = np.maximum(hl, np.maximum(hc, lc))

    atr = pd.Series(tr).rolling(window=window).mean()

    return atr


# ---------------------------------------------------------
# APPLY ALL INDICATORS (HELPER)
# ---------------------------------------------------------

def apply_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all technical indicators to dataframe

    Adds:
        SMA_5
        SMA_20
        RSI
        ATR

    Returns:
        Updated DataFrame
    """

    df = df.copy()

    df["SMA_5"] = compute_sma(df["Close"], 5)
    df["SMA_20"] = compute_sma(df["Close"], 20)
    df["RSI"] = compute_rsi(df["Close"], 14)
    df["ATR"] = compute_atr(df, 14)

    return df