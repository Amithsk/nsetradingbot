import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse
from pathlib import Path
import traceback


# --- CONFIGURATION ---
def load_config():
    password = os.getenv('MYSQL_PASSWORD')
    encoded_pw = urllib.parse.quote_plus(password)  # Properly escape special characters
    DATABASE_URL = f"mysql+pymysql://root:{encoded_pw}@localhost/nifty"
    engine = create_engine(DATABASE_URL)
    OUTPUT_ROOT = "./Output"
    return OUTPUT_ROOT, engine


def prev_trading_day(dt):
    dt -= timedelta(days=1)
    while dt.weekday() >= 5:
        dt -= timedelta(days=1)
    return dt


# --- HELPERS ---
def extract_date_from_folder(folder: Path):
    """Extract date string (yyyymmdd) from folder name if valid"""
    try:
        datetime.strptime(folder.name, "%Y%m%d")
        return folder.name
    except ValueError:
        return None


# 1) Load raw prices
def load_prices(OUTPUT_ROOT, conn, date_str):
    try:
        sample = list(Path(OUTPUT_ROOT, date_str, "backward").glob(f"nifty_*{date_str}.csv"))[0].as_posix()
        df = pd.read_csv(sample)
        df['Datetime'] = pd.to_datetime(df['Datetime'])

        price_df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'SMA_5', 'SMA_20', 'RSI', 'ATR']]
        price_df.rename(columns={'Datetime': 'Date'}, inplace=True)
        price_df = price_df.drop_duplicates(subset=["Date"])

        csv_start, csv_end = price_df['Date'].min(), price_df['Date'].max()
        query = "SELECT Date FROM nifty_prices WHERE Date BETWEEN %s AND %s"
        existing = pd.read_sql(query, conn, params=[csv_start, csv_end])

        if not existing.empty:
            price_df = price_df[~price_df["Date"].isin(existing["Date"])]

        if not price_df.empty:
            price_df.to_sql("nifty_prices", conn, if_exists="append", index=False)
            print(f"[{date_str}] Inserted {len(price_df)} new price rows")
        else:
            print(f"[{date_str}] No new price rows to insert")

    except Exception as e:
        print(f"[{date_str}] Error in load_prices: {e}")


# 2) Load predictions
def load_predictions(OUTPUT_ROOT, conn, date_str):
    try:
        # backward
        back_files = list(Path(OUTPUT_ROOT, date_str, "backward").glob(f"nifty_*{date_str}.csv"))
        for f in back_files:
            f = f.as_posix()
            model = os.path.basename(f).split('_')[1]
            df = pd.read_csv(f, parse_dates=['Datetime'])
            preds = df[['Datetime', 'Prediction', 'Predicted_Price']].copy()
            preds['model_name'] = model
            preds['is_forward'] = False
            preds.rename(columns={'Datetime': 'date', 'Prediction': 'predicted_dir'}, inplace=True)
            preds.to_sql('predictions', conn, if_exists='append', index=False)

        # forward
        fwd_files = list(Path(OUTPUT_ROOT, date_str, "forward").glob(f"nifty_*{date_str}.csv"))
        for f in fwd_files:
            model = os.path.basename(f).split('_')[1]
            df = pd.read_csv(f, parse_dates=['Datetime'])
            preds = df[['Datetime', 'Prediction', 'Predicted_Price']].copy()
            preds['model_name'] = model
            preds['is_forward'] = True
            preds.rename(columns={'Datetime': 'date', 'Prediction': 'predicted_dir'}, inplace=True)
            preds.to_sql('predictions', conn, if_exists='append', index=False)

    except Exception as e:
        print(f"[{date_str}] Error in load_predictions: {e}")
        traceback.print_exc()


# 3) Load comparisons
def load_comparisons(OUTPUT_ROOT, conn, date_str):
    try:
        cmp_files = list(Path(OUTPUT_ROOT, date_str, "evaluation").glob(f"*comparison*{date_str}.csv"))
        for f in cmp_files:
            f = f.as_posix()
            model = os.path.basename(f).split('_')[0]
            df = pd.read_csv(f, parse_dates=['Datetime'])
            df['model_name'] = model
            df.rename(columns={'Datetime': 'date', 'true_direction': 'actual_dir'}, inplace=True)
            df[['date', 'model_name', 'actual_dir', 'Prediction', 'was_correct', 'error_mag']] \
                .rename(columns={'Prediction': 'predicted_dir'}) \
                .to_sql('comparisons', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"[{date_str}] Error in load_comparisons: {e}")
        traceback.print_exc()


# 4) Load daily summaries
def load_daily_summary(OUTPUT_ROOT, conn, date_str):
    try:
        sum_file = list(Path(OUTPUT_ROOT, date_str, "evaluation").glob(f"evaluation_summary*{date_str}.csv"))[0].as_posix()
        df = pd.read_csv(sum_file)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df.rename(columns={'date': 'summary_date', 'model': 'model_name'}, inplace=True)
        df.to_sql('model_daily_summary', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"[{date_str}] Error in load_daily_summary: {e}")
        traceback.print_exc()


# 5) Load forward summary
def load_forward_summary(OUTPUT_ROOT, conn, date_str):
    try:
        sum_file = list(Path(OUTPUT_ROOT, date_str, "forward").glob(f"forward_summary*{date_str}.csv"))[0].as_posix()
        df = pd.read_csv(sum_file)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df.rename(columns={
            'date': 'summary_date',
            'model': 'model_name',
            'bullish': 'bullish_count',
            'bearish': 'bearish_count',
            'predicted_close_mean': 'avg_pred_price'
        }, inplace=True)
        df.to_sql('forward_summary', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"[{date_str}] Error in load_forward_summary: {e}")
        traceback.print_exc()


# --- DRIVER FUNCTION ---
def process_date(OUTPUT_ROOT, conn, date_str):
    print(f"\n=== Processing {date_str} ===")
    load_prices(OUTPUT_ROOT, conn, date_str)
    load_predictions(OUTPUT_ROOT, conn, date_str)
    load_comparisons(OUTPUT_ROOT, conn, date_str)
    load_daily_summary(OUTPUT_ROOT, conn, date_str)
    load_forward_summary(OUTPUT_ROOT, conn, date_str)


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    OUTPUT_ROOT, engine = load_config()
    Session = sessionmaker(bind=engine)
    session = Session()
    conn = session.connection()

    try:
        # MODE 1: Single date (manual run)
        # date_str = datetime.now().strftime("%Y%m%d")
        # process_date(OUTPUT_ROOT, conn, date_str)

        # MODE 2: Process ALL date folders under OUTPUT_ROOT
        for folder in Path(OUTPUT_ROOT).iterdir():
            if folder.is_dir():
                date_str = extract_date_from_folder(folder)
                if date_str:
                    process_date(OUTPUT_ROOT, conn, date_str)

        # session.commit()
        print("Data load finished")

    except Exception as e:
        print("Error occurred:", e)
        session.rollback()
    finally:
        session.close()
