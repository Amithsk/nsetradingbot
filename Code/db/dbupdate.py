import os
import glob
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse
from pathlib import Path
import traceback
import cryptography


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

# use today’s date for backward/test, prev_day for forward
today = datetime.now()
prev_day = prev_trading_day(today)
today_str = today.strftime("%Y%m%d")
prev_str = prev_day.strftime("%Y%m%d")
today_str ='20250718'

# 1) Load raw prices
def load_prices(OUTPUT_ROOT, conn):

    try:
        sample= list(Path(OUTPUT_ROOT, today_str, "backward").glob("nifty_*.csv"))[0].as_posix()
        df = pd.read_csv(sample)
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        price_df = df[['Datetime','Open','High','Low','Close','Volume','SMA_5','SMA_20','RSI','ATR']].drop_duplicates()
        price_df.rename(columns={'Datetime': 'Date'}, inplace=True)
        price_df.to_sql('nifty_prices', conn, if_exists='replace', index=False)
    
    except Exception as e:
        print("An error occurred while loading prices:")
        print(f"Error type: {type(e).__name__}")
        traceback.print_exc()
        raise  


# 2) Load predictions
def load_predictions(OUTPUT_ROOT, conn):

    # backward
    try:
        back_files= list(Path(OUTPUT_ROOT, today_str, "backward").glob("nifty_*.csv"))

        for f in back_files:
            f=f.as_posix()
            model = os.path.basename(f).split('_')[1]
            df = pd.read_csv(f, parse_dates=['Datetime'])
            preds = df[['Datetime','Prediction','Predicted_Price']].copy()
            preds['model_name'] = model
            preds['is_forward'] = False
            preds.rename(columns={'Datetime': 'date', 'Prediction': 'predicted_dir'}, inplace=True)
            preds.to_sql('predictions', conn, if_exists='append', index=False)

    # forward
        fwd_files= list(Path(OUTPUT_ROOT, today_str, "forward").glob("nifty_*.csv"))
        for f in fwd_files:
            model = os.path.basename(f).split('_')[1]
            df = pd.read_csv(f, parse_dates=['Datetime'])
            preds = df[['Datetime','Prediction','Predicted_Price']].copy()
            preds['model_name'] = model
            preds['is_forward'] = True
            preds.rename(columns={'Datetime': 'date', 'Prediction': 'predicted_dir'}, inplace=True)
            preds.to_sql('predictions', conn, if_exists='append', index=False)
    except Exception as e:
        print("An error occurred while loading predictions:")
        print(f"Error type: {type(e).__name__}")
        traceback.print_exc()
        raise  

# 3) Load comparisons
def load_comparisons(OUTPUT_ROOT, conn):

    try:
        cmp_files=list(Path(OUTPUT_ROOT, today_str, "evaluation").glob("*_comparison_*.csv}"))
        for f in cmp_files:
            f=f.as_posix()
            model = os.path.basename(f).split('_')[0]
            df = pd.read_csv(f, parse_dates=['Datetime'])
            df['model_name'] = model
            df.rename(columns={'Datetime': 'date', 'true_direction': 'actual_dir'}, inplace=True)
            df[['date','model_name','actual_dir','Prediction','was_correct','error_mag']]\
            .rename(columns={'Prediction': 'predicted_dir'})\
            .to_sql('comparisons', conn, if_exists='append', index=False)
    except Exception as e:
        print("An error occurred while loading comparisons:")
        print(f"Error type: {type(e).__name__}")
        traceback.print_exc()
        raise  

# 4) Load daily summaries
def load_daily_summary(OUTPUT_ROOT, conn):

    try:
        sum_file= list(Path(OUTPUT_ROOT, today_str, "evaluation").glob("evaluation_summary_*.csv"))[0].as_posix()
        df = pd.read_csv(sum_file)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df.rename(columns={'date': 'summary_date', 'model': 'model_name'}, inplace=True)
        df.to_sql('model_daily_summary', conn, if_exists='append', index=False)

    except Exception as e:
        print("An error occurred while loading daily summaries:")
        print(f"Error type: {type(e).__name__}")
        traceback.print_exc()
        raise  

# 5) Load forward_summary
def load_forward_summary(OUTPUT_ROOT, conn):

    try:
        sum_file= list(Path(OUTPUT_ROOT, today_str, "forward").glob("forward_summary*.csv"))[0].as_posix()
        df = pd.read_csv(sum_file)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df.rename(columns={'date': 'summary_date', 'model': 'model_name','bullish':'bullish_count','bearish':'bearish_count','predicted_close_mean':'avg_pred_price'}, inplace=True)
        df.to_sql('forward_summary', conn, if_exists='append', index=False)

    except Exception as e:
        print("An error occurred while loading forward summaries:")
        print(f"Error type: {type(e).__name__}")
        traceback.print_exc()
        raise  
    

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    OUTPUT_ROOT, engine = load_config()
    Session = sessionmaker(bind=engine)
    session = Session()
    conn = session.connection()

    try:
        # Manual control for first run
        load_prices(OUTPUT_ROOT, conn)
        load_predictions(OUTPUT_ROOT, conn)
        load_comparisons(OUTPUT_ROOT, conn)
        load_daily_summary(OUTPUT_ROOT, conn)
        load_forward_summary(OUTPUT_ROOT, conn)

        # Uncomment after checking:
        # session.commit()
        print("Data loaded ")
    except Exception as e:
        print("Error occurred:", e)
        session.rollback()
    finally:
        session.close()
