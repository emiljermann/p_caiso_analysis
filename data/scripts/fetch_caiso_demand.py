import requests as re
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import time
import argparse
import os

"""
Usage:

python ./fetch_caiso_demand.py --start 2025-01-01 --end 2026-01-01 --tag 2025
"""

FUELSOURCE_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/fuelsource.csv"
CO2_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/co2.csv"
DEMAND_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/demand.csv"

CUR_DIR = os.path.dirname(__file__)

API_SLEEP = 1
MAX_TRIES = 3

def fetch_demand_day(date : datetime) -> pd.DataFrame:
    date_str = date.strftime("%Y%m%d")
    response = re.get(DEMAND_URL_TEMPLATE.format(date=date_str))
    
    if response.status_code != 200:
        raise ValueError(f"Expected response code 200, got {response.status_code}")
    
    df = pd.read_csv(StringIO(response.text))

    df.columns = df.columns.str.lower()

    return df

def fetch_demand(start : datetime, end : datetime) -> pd.DataFrame:
    """
    Fetches co2 emissions data
    """
    if start > end:
        raise ValueError(f"Start time ({start}) must be before end time ({end})")
    
    frames = list()

    date = start
    while date != end:
        print(f"Querying day {date.strftime("%Y-%m-%d")}: ", end="")

        tries = 0
        while True:
            try:
                frame = fetch_demand_day(date)
                break
            except ValueError as e:
                if tries >= MAX_TRIES:
                    print(f"Max tries reached")
                    raise e
                
                print(f"Error fetching, retrying... {tries}/{MAX_TRIES}")
                tries += 1

        print(f" {len(frame)} rows returned")

        forecast_cols = ['day ahead forecast', 'hour ahead forecast', 'current demand']
        frame["datetime"] = pd.to_datetime( date.strftime("%Y-%m-%d") + " " + frame["time"].astype(str) ) 
        frame = pd.melt(
            frame, 
            id_vars=['datetime'], 
            value_vars=forecast_cols, 
            var_name='forecast', 
            value_name='MW',
            ignore_index=True) 
        

        frames.append(frame)
        date += timedelta(days=1)

        time.sleep(API_SLEEP)
    
    return pd.concat(frames, ignore_index=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch all CAISO Demand emissions outlook data between two dates and save to csv")
    parser.add_argument("--start", "-s", default="2025-01-01", help="Start date (inclusive) -> default: 2025-01-01")
    parser.add_argument("--end", "-e", default="2025-01-05", help="End date (exclusive) -> default: 2025-01-05")
    
    args = parser.parse_args()
    
    print(f"Querying CAISO Demand between {args.start} and {args.end}")

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    df = fetch_demand(start_date, end_date)

    df = df.groupby(by=["datetime", "forecast"]).mean()
    df = df.sort_values(by=["datetime", "forecast"]).reset_index()

    filename = f"caiso_demand_{args.start.replace('-', '')}_{args.end.replace('-', '')}.csv"
    filepath = os.path.join(CUR_DIR, '..', filename)
    
    print(df.head(), '\n')
    print(f"Saving as {filename}")
    df.to_csv(filepath, index=False, mode='w')