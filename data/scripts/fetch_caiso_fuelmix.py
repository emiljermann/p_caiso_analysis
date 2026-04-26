import requests as re
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import time
import argparse
import os

"""
Usage:

python ./fetch_caiso_fuelmix.py --start 2025-01-01 --end 2026-01-01
"""

FUELSOURCE_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/fuelsource.csv"
CO2_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/co2.csv"
DEMAND_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/demand.csv"

CUR_DIR = os.path.dirname(__file__)

API_SLEEP = 1
MAX_TRIES = 3

def fetch_fuelmix_day(date : datetime) -> pd.DataFrame:
    date_str = date.strftime("%Y%m%d")
    response = re.get(FUELSOURCE_URL_TEMPLATE.format(date=date_str))
    
    if response.status_code != 200:
        raise ValueError(f"Expected response code 200, got {response.status_code}")
    
    df = pd.read_csv(StringIO(response.text))

    df.columns = df.columns.str.lower()

    return df

def fetch_fuelmix(start : datetime, end : datetime) -> pd.DataFrame:
    """
    Fetches fuelsource data
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
                frame = fetch_fuelmix_day(date)
                break
            except ValueError as e:
                if tries >= MAX_TRIES:
                    print(f"Max tries reached")
                    raise e
                
                print(f"Error fetching, retrying... {tries}/{MAX_TRIES}")
                tries += 1
        print(f" {len(frame)} rows returned")

        fuel_cols = ['solar', 'wind', 'geothermal', 'biomass', 'biogas', 
                    'small hydro', 'coal', 'nuclear', 'natural gas', 
                    'large hydro', 'batteries', 'imports', 'other']
        frame["datetime"] = pd.to_datetime( date.strftime("%Y-%m-%d") + " " + frame["time"].astype(str) ) 
        frame = pd.melt(
            frame, 
            id_vars=['datetime'], 
            value_vars=fuel_cols, 
            var_name='fuel_type', 
            value_name='MW',
            ignore_index=True) 
        

        frames.append(frame)
        date += timedelta(days=1)

        time.sleep(API_SLEEP)
    
    return pd.concat(frames, ignore_index=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch all CAISO fuel mix outlook data between two dates and save to csv")
    parser.add_argument("--start", "-s", default="2025-01-01", help="Start date (inclusive) -> default: 2025-01-01")
    parser.add_argument("--end", "-e", default="2025-01-05", help="End date (exclusive) -> default: 2025-01-05")
    
    args = parser.parse_args()

    print(f"Querying CAISO fuel mix between {args.start} and {args.end}")
    
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    df = fetch_fuelmix(start_date, end_date)

    # df['datetime'] = df['datetime'].dt.floor('h')
    # df = df.groupby(by=["datetime", "fuel_type"]).mean()
    # df = df.sort_values(by=["datetime", "fuel_type"]).reset_index()

    df = df.sort_values(by=["datetime", "fuel_type"]).reset_index(drop=True)

    filename = f"caiso_fuelmix_{args.start.replace('-', '')}_{args.end.replace('-', '')}.csv"
    filepath = os.path.join(CUR_DIR, '..', filename)

    print(df.head(), '\n')
    print(f"Saving as {filename}")
    df.to_csv(filepath, index=False, mode='w')