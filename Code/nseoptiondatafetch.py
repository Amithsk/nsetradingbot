
#!/usr/bin/env python3
import os
import sys
import pytz
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from nsepython import nse_optionchain_scrapper, nse_marketStatus
from utils.nseholiday import nseholiday

IST = pytz.timezone("Asia/Kolkata")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
ENV_PATH = BASE_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(ENV_PATH)

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "NIFTY,BANKNIFTY").split(",") if s.strip()]

def now_ist():
    return datetime.now(IST)

def in_market_hours(ts: datetime) -> bool:
    start = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    end   = ts.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= ts <= end

def nse_is_open(file_date_obj) -> bool:
    today= datetime.now()
    if today.weekday() == 5 or 6:
        print(f"{today} is a weekend,so no NSE trading")
        return False
    if nseholiday(file_date_obj):
        print(f"{file_date_obj} was an NSE holiday. Skipping backward script.")
        return False
    return True

def flatten_option_chain(underlying: str, oc_json: dict, ts: datetime) -> pd.DataFrame:
    rows = []
    r = oc_json.get("records", {})
    uval = r.get("underlyingValue", None)
    for d in r.get("data", []):
        strike = d.get("strikePrice")
        expiry = d.get("expiryDate")
        for opt_type in ("CE", "PE"):
            leg = d.get(opt_type)
            if not leg:
                continue
            rows.append({
                "timestamp": ts.isoformat(),
                "underlying": underlying,
                "expiry": expiry,
                "strike": strike,
                "option_type": opt_type,
                "last_price": leg.get("lastPrice"),
                "change": leg.get("change"),
                "percent_change": leg.get("pChange"),
                "volume": leg.get("totalTradedVolume"),
                "open_interest": leg.get("openInterest"),
                "change_in_oi": leg.get("changeinOpenInterest"),
                "implied_vol": leg.get("impliedVolatility"),
                "bid_qty": leg.get("bidQty"),
                "bid_price": leg.get("bidprice"),
                "ask_price": leg.get("askPrice"),
                "ask_qty": leg.get("askQty"),
                "underlying_value": uval,
            })
    df = pd.DataFrame(rows)
    cols = ["timestamp","underlying","expiry","strike","option_type",
            "last_price","change","percent_change","volume","open_interest",
            "change_in_oi","implied_vol","bid_qty","bid_price","ask_price","ask_qty",
            "underlying_value"]
    if not df.empty:
        df = df[cols]
    return df

def append_csv(underlying: str, df: pd.DataFrame, ts: datetime):
    fname = f"{underlying}_{ts.date().isoformat()}.csv"
    fpath = DATA_DIR / fname
    header = not fpath.exists()
    df.to_csv(fpath, mode="a", header=header, index=False)

def main():
    ts = now_ist()
    if not nse_is_open():
        print("NSE market not open; skipping.")
        return
    
    if not in_market_hours(ts):
        print("Market hours closed; skipping.")
        return
    
    

    captured = 0
    for sym in SYMBOLS:
        try:
            oc = nse_optionchain_scrapper(sym.upper())
            df = flatten_option_chain(sym.upper(), oc, ts)
            if df.empty:
                print(f"{sym}: empty option chain; skip.")
                continue
            append_csv(sym.upper(), df, ts)
            captured += len(df)
            print(f"{sym}: appended {len(df)} rows.")
        except Exception as e:
            print(f"ERROR fetching {sym}: {e}", file=sys.stderr)

    if captured == 0:
        print("No data captured.")

if __name__ == "__main__":
    main()
