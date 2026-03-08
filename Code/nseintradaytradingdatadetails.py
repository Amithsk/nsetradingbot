# ============================================================
# File: generate_intraday_metrics.py
# ============================================================

import pandas as pd


def calculate_vwap(df):
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    df["VWAP"] = (typical_price * df["Volume"]).cumsum() / df["Volume"].cumsum()
    return df


def calculate_gap_percent(open_price, prev_close):
    return ((open_price - prev_close) / prev_close) * 100


def calculate_gap_hold(df, prev_close):

    open_price = df.iloc[0]["Open"]
    intraday_low = df["Low"].min()
    intraday_high = df["High"].max()

    if open_price > prev_close:
        return "YES" if intraday_low >= prev_close else "NO"

    elif open_price < prev_close:
        return "YES" if intraday_high <= prev_close else "NO"

    else:
        return "NO_GAP"


def calculate_price_vs_vwap(df):

    last_close = df.iloc[-1]["Close"]
    last_vwap = df.iloc[-1]["VWAP"]

    if last_close > last_vwap:
        return "ABOVE_VWAP"

    elif last_close < last_vwap:
        return "BELOW_VWAP"

    return "AT_VWAP"


def calculate_structure(df):

    highs = df["High"].values
    lows = df["Low"].values

    higher_highs = all(x < y for x, y in zip(highs, highs[1:]))
    higher_lows = all(x < y for x, y in zip(lows, lows[1:]))

    lower_highs = all(x > y for x, y in zip(highs, highs[1:]))
    lower_lows = all(x > y for x, y in zip(lows, lows[1:]))

    if higher_highs and higher_lows:
        return "LONG_VALID"

    if lower_highs and lower_lows:
        return "SHORT_VALID"

    return "INVALID"


def main():

    print("\n===== Intraday Metrics Generator =====\n")

    csv_path = input("Enter CSV file path: ").strip()
    prev_close = float(input("Enter Previous Close value: ").strip())

    df = pd.read_csv(csv_path)

    # Clean Volume
    df["Volume"] = df["Volume"].astype(str).str.replace(",", "")
    df["Volume"] = df["Volume"].astype(float)

    # Clean Date column
    df["Date"] = df["Date"].str.replace(r" GMT.*", "", regex=True)
    df["Date"] = pd.to_datetime(df["Date"], format="%a %b %d %Y %H:%M:%S")

    # Sort by Date
    df = df.sort_values("Date").reset_index(drop=True)

    # Calculate VWAP
    df = calculate_vwap(df)

    open_price = df.iloc[0]["Open"]

    gap_percent = calculate_gap_percent(open_price, prev_close)
    gap_hold = calculate_gap_hold(df, prev_close)
    price_vs_vwap = calculate_price_vs_vwap(df)
    structure_valid = calculate_structure(df)

    print("\n===== Results =====\n")

    print(f"Gap % : {round(gap_percent,2)}")
    print(f"Gap Hold : {gap_hold}")
    print(f"Price vs VWAP : {price_vs_vwap}")
    print(f"Structure Valid : {structure_valid}")


if __name__ == "__main__":
    main()