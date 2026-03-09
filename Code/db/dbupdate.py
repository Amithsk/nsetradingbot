# To update the Nifty tables

import os
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse
from pathlib import Path
import traceback

# NEW: IST + holiday hook
from zoneinfo import ZoneInfo
try:
    from nsebackwardbot import nseholiday
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
# HELPERS
# ------------------------------------------------

def extract_date_from_folder(folder: Path):

    try:

        datetime.strptime(folder.name, "%Y%m%d")

        return folder.name

    except ValueError:

        return None


# ------------------------------------------------
# LOAD PRICES
# ------------------------------------------------

def load_prices(OUTPUT_ROOT, conn, _date_str_unused):

    try:

        matches = list(Path(OUTPUT_ROOT, RUN_DATE_STR, "backward").glob(f"nifty_*{YDAY_STR}.csv"))

        if not matches:

            print(f"No file found for {YDAY_STR} load prices, skipping...")

            return None


        sample = matches[0].as_posix()

        df = pd.read_csv(sample)

        df['Datetime'] = pd.to_datetime(df['Datetime'])


        price_df = df[['Datetime','Open','High','Low','Close','Volume','SMA_5','SMA_20','RSI','ATR']].copy()

        price_df.rename(columns={'Datetime': 'Date'}, inplace=True)

        price_df = price_df.drop_duplicates(subset=["Date"])


        csv_start = price_df['Date'].min()

        csv_end = price_df['Date'].max()


        csv_start = csv_start.strftime("%Y-%m-%d")

        csv_end = csv_end.strftime("%Y-%m-%d")


        query = "SELECT Date FROM nifty_prices WHERE Date BETWEEN %s AND %s"

        existing = pd.read_sql(query, conn, params=(csv_start, csv_end))


        if not existing.empty:

            price_df = price_df[~price_df["Date"].isin(existing["Date"])]


        if not price_df.empty:

            price_df.to_sql("nifty_prices", conn, if_exists="append", index=False)

            print(f"[{RUN_DATE_STR}] Inserted {len(price_df)} new price rows (files dated {YDAY_STR})")

        else:

            print(f"[{RUN_DATE_STR}] No new price rows to insert")


    except Exception as e:

        print(f"[{RUN_DATE_STR}] Error in load_prices: {e}")


# ------------------------------------------------
# LOAD PREDICTIONS
# ------------------------------------------------

def load_predictions(OUTPUT_ROOT, conn, _date_str_unused):

    try:

        back_files = list(Path(OUTPUT_ROOT, RUN_DATE_STR, "backward").glob(f"nifty_*{YDAY_STR}.csv"))

        if not back_files:

            print(f"No file found for {YDAY_STR} prediction files, skipping...")

            return None


        for f in back_files:

            f = f.as_posix()

            model = os.path.basename(f).split('_')[1]

            df = pd.read_csv(f, parse_dates=['Datetime'])


            preds = df[['Datetime','Prediction','Predicted_Price']].copy()

            preds['Datetime'] = preds['Datetime'].dt.tz_localize(None)

            preds['model_name'] = model

            preds['is_forward'] = False

            preds.rename(columns={'Datetime': 'date','Prediction':'predicted_dir'}, inplace=True)


            existing = pd.read_sql("SELECT date,model_name,is_forward FROM predictions", conn)


            preds = preds.merge(existing,on=["date","model_name","is_forward"],how="left",indicator=True)

            preds = preds[preds["_merge"] == "left_only"].drop(columns=["_merge"])


            if not preds.empty:

                preds.to_sql('predictions',conn,if_exists='append',index=False)

                print(f"[{RUN_DATE_STR}] Inserted {len(preds)} rows for {model} backward predictions")


        fwd_files = list(Path(OUTPUT_ROOT, RUN_DATE_STR, "forward").glob(f"nifty_*{RUN_DATE_STR}.csv"))

        if not fwd_files:

            print(f"No forward prediction files found")

            return None


        for f in fwd_files:

            model = os.path.basename(f).split('_')[1]

            df = pd.read_csv(f, parse_dates=['Datetime'])


            preds = df[['Datetime','Prediction','Predicted_Price']].copy()

            preds['Datetime'] = preds['Datetime'].dt.tz_localize(None)

            preds['model_name'] = model

            preds['is_forward'] = True

            preds.rename(columns={'Datetime':'date','Prediction':'predicted_dir'}, inplace=True)


            existing = pd.read_sql("SELECT date,model_name,is_forward FROM predictions", conn)


            preds = preds.merge(existing,on=["date","model_name","is_forward"],how="left",indicator=True)

            preds = preds[preds["_merge"] == "left_only"].drop(columns=["_merge"])


            if not preds.empty:

                preds.to_sql('predictions',conn,if_exists='append',index=False)

                print(f"[{RUN_DATE_STR}] Inserted {len(preds)} rows for {model} forward predictions")


    except Exception as e:

        print(f"[{RUN_DATE_STR}] Error in load_predictions: {e}")

        traceback.print_exc()


# ------------------------------------------------
# LOAD COMPARISONS
# ------------------------------------------------

def load_comparisons(OUTPUT_ROOT, conn, _date_str_unused):

    try:

        cmp_files = list(Path(OUTPUT_ROOT, RUN_DATE_STR, "evaluation").glob("*comparison*.csv"))

        if not cmp_files:

            print("No comparison files found")

            return None


        for f in cmp_files:

            f = f.as_posix()

            model = os.path.basename(f).split('_')[0]

            df = pd.read_csv(f, parse_dates=['Datetime'])

            df['Datetime'] = df['Datetime'].dt.tz_localize(None)

            df['model_name'] = model

            df.rename(columns={'Datetime':'date','true_direction':'actual_dir'}, inplace=True)


            existing = pd.read_sql("SELECT date,model_name FROM comparisons", conn)


            df = df.merge(existing,on=["date","model_name"],how="left",indicator=True)

            df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])


            if not df.empty:

                df[['date','model_name','actual_dir','Prediction','was_correct','error_mag']]\
                    .rename(columns={'Prediction':'predicted_dir'})\
                    .to_sql('comparisons',conn,if_exists='append',index=False)

                print(f"[{RUN_DATE_STR}] Inserted {len(df)} comparison rows for {model}")


    except Exception as e:

        print(f"[{RUN_DATE_STR}] Error in load_comparisons: {e}")

        traceback.print_exc()


# ------------------------------------------------
# LOAD DAILY SUMMARY
# ------------------------------------------------

def load_daily_summary(OUTPUT_ROOT, conn, _date_str_unused):

    try:
        daily_matches = list(Path(OUTPUT_ROOT, RUN_DATE_STR, "evaluation").glob("evaluation_summary*.csv"))
        if not daily_matches:
            print("No evaluation summary file found")
            return None
        sum_file = daily_matches[0].as_posix()
        try:
            df = pd.read_csv(sum_file)
        except pd.errors.EmptyDataError:
            print(f"[{RUN_DATE_STR}] Evaluation summary file is empty, skipping.")
            return None
        df['date'] = pd.to_datetime(df['date']).dt.date
        df.rename(columns={'date':'summary_date','model':'model_name'}, inplace=True)
        existing = pd.read_sql("SELECT summary_date,model_name FROM model_daily_summary", conn)
        df = df.merge(existing,on=["summary_date","model_name"],how="left",indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
        if not df.empty:
            df.to_sql('model_daily_summary',conn,if_exists='append',index=False)
            print(f"[{RUN_DATE_STR}] Inserted {len(df)} daily summary rows")
    except Exception as e:
        print(f"[{RUN_DATE_STR}] Error in load_daily_summary: {e}")
        traceback.print_exc()


# ------------------------------------------------
# LOAD FORWARD SUMMARY
# ------------------------------------------------

def load_forward_summary(OUTPUT_ROOT, conn, _date_str_unused):

    try:

        forward_matches = list(Path(OUTPUT_ROOT, RUN_DATE_STR, "forward").glob("forward_summary*.csv"))

        if not forward_matches:

            print("No forward summary file found")

            return None


        sum_file = forward_matches[0].as_posix()

        df = pd.read_csv(sum_file)

        df['date'] = pd.to_datetime(df['date']).dt.date

        df.rename(columns={
            'date':'summary_date',
            'model':'model_name',
            'bullish':'bullish_count',
            'bearish':'bearish_count',
            'predicted_close_mean':'avg_pred_price'
        }, inplace=True)


        existing = pd.read_sql("SELECT summary_date,model_name FROM forward_summary", conn)


        df = df.merge(existing,on=["summary_date","model_name"],how="left",indicator=True)

        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])


        if not df.empty:

            df.to_sql('forward_summary',conn,if_exists='append',index=False)

            print(f"[{RUN_DATE_STR}] Inserted {len(df)} forward summary rows")


    except Exception as e:

        print(f"[{RUN_DATE_STR}] Error in load_forward_summary: {e}")

        traceback.print_exc()


# ------------------------------------------------
# DRIVER
# ------------------------------------------------

def process_date(OUTPUT_ROOT, conn, date_str):

    print(f"\nProcessing folder {RUN_DATE_STR} (previous trading day {YDAY_STR})")

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
        run_day = datetime.strptime(RUN_DATE_STR,"%Y%m%d").date()
        YDAY_STR = run_day.strftime("%Y%m%d")
        process_date(OUTPUT_ROOT, conn, RUN_DATE_STR)
        session.commit()
        print("DB update completed successfully")


    except Exception as e:

        print("Error occurred:", e)

        traceback.print_exc()

        session.rollback()

    finally:

        session.close()