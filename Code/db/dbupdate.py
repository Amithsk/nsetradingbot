# To update the Nifty tables

import os
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse
from pathlib import Path
import traceback

from zoneinfo import ZoneInfo
try:
    from nsedatadailydownload import nseholiday
except Exception:
    nseholiday = None


# ------------------------------------------------
# CONFIGURATION
# ------------------------------------------------

def load_config():

    password = os.getenv('MYSQL_PASSWORD')
    encoded_pw = urllib.parse.quote_plus(password)

    DATABASE_URL = f"mysql+pymysql://root:{encoded_pw}@localhost/nifty"

    engine = create_engine(DATABASE_URL)

    OUTPUT_ROOT = "./Output"

    return OUTPUT_ROOT, engine


# ------------------------------------------------
# HOLIDAY CHECK
# ------------------------------------------------

def prev_trading_day(dt: date):

    dt -= timedelta(days=1)

    while dt.weekday() >= 5 or _is_nse_holiday(dt):

        dt -= timedelta(days=1)

    return dt


def _is_nse_holiday(d: date) -> bool:

    if nseholiday is None:
        return False

    for fmt in ("%Y%m%d", "%Y-%m-%d", "%d%m%Y", "%d-%m-%Y"):

        try:
            res = nseholiday(d.strftime(fmt))

            if isinstance(res, bool):
                return res

            if isinstance(res, str) and res.strip().lower() in {"holiday", "true", "yes"}:
                return True

        except Exception:
            continue

    return False


# ------------------------------------------------
# GLOBAL DATE TOKENS
# ------------------------------------------------

RUN_DATE_STR = None
YDAY_STR = None


# ------------------------------------------------
# LOAD PRICES (UPDATED ONLY HERE)
# ------------------------------------------------

def load_prices(OUTPUT_ROOT, conn, _date_str_unused):

    try:

        file_path = Path(OUTPUT_ROOT, RUN_DATE_STR, f"{RUN_DATE_STR}data.csv")

        if not file_path.exists():
            print(f"No data file found for {RUN_DATE_STR}, skipping...")
            return None

        df = pd.read_csv(file_path)

        df['Datetime'] = pd.to_datetime(df['Datetime'])

        price_df = df[['Datetime','Open','High','Low','Close','Volume']].copy()

        price_df.rename(columns={'Datetime': 'Date'}, inplace=True)

        price_df = price_df.drop_duplicates(subset=["Date"])

        csv_start = price_df['Date'].min().strftime("%Y-%m-%d")
        csv_end = price_df['Date'].max().strftime("%Y-%m-%d")

        query = "SELECT Date FROM nifty_prices WHERE Date BETWEEN %s AND %s"

        existing = pd.read_sql(query, conn, params=(csv_start, csv_end))

        if not existing.empty:
            price_df = price_df[~price_df["Date"].isin(existing["Date"])]

        if not price_df.empty:

            price_df.to_sql("nifty_prices", conn, if_exists="append", index=False)

            print(f"[{RUN_DATE_STR}] Inserted {len(price_df)} new price rows")

        else:

            print(f"[{RUN_DATE_STR}] No new price rows to insert")

    except Exception as e:

        print(f"[{RUN_DATE_STR}] Error in load_prices: {e}")


# ------------------------------------------------
# DISABLED ML FUNCTIONS (KEPT FOR STRUCTURE)
# ------------------------------------------------

def load_predictions(*args, **kwargs):
    print("Prediction pipeline removed - skipping...")


def load_comparisons(*args, **kwargs):
    print("Comparison pipeline removed - skipping...")


def load_daily_summary(*args, **kwargs):
    print("Daily summary pipeline removed - skipping...")


def load_forward_summary(*args, **kwargs):
    print("Forward summary pipeline removed - skipping...")


# ------------------------------------------------
# DRIVER
# ------------------------------------------------

def process_date(OUTPUT_ROOT, conn, date_str):

    print(f"\nProcessing folder {RUN_DATE_STR}")

    load_prices(OUTPUT_ROOT, conn, date_str)

    load_predictions(OUTPUT_ROOT, conn, date_str)

    load_comparisons(OUTPUT_ROOT, conn, date_str)

    load_daily_summary(OUTPUT_ROOT, conn, date_str)

    load_forward_summary(OUTPUT_ROOT, conn, date_str)


# ------------------------------------------------
# MAIN
# ------------------------------------------------

if __name__ == "__main__":

    OUTPUT_ROOT, engine = load_config()

    Session = sessionmaker(bind=engine)

    session = Session()

    conn = session.connection()

    try:

        folder_input = input("Enter folder name (YYYYMMDD): ").strip()

        folder_path = Path(OUTPUT_ROOT) / folder_input

        if not folder_path.exists():
            print("Folder not found")
            raise SystemExit(1)

        RUN_DATE_STR = folder_input

        process_date(OUTPUT_ROOT, conn, RUN_DATE_STR)

        session.commit()

        print("DB update completed successfully")

    except Exception as e:

        print("Error occurred:", e)

        traceback.print_exc()

        session.rollback()

    finally:

        session.close()