import requests as re
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
from time import sleep

"""
Usage:

python ./fetch_caiso_outlooks.py --start 2025-01-01 --end 2026-01-01 --tag 2025
"""

FUELSOURCE_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/fuelsource.csv"
CO2_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/co2.csv"
DEMAND_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/demand.csv"

API_SLEEP = 1

# date = datetime("2025-01-01")
# r = re.get(FUELSOURCE_URL_TEMPLATE.format(date="20250101"))
# print(r.status_code)
# df = pd.read_csv(StringIO(r.text))
# print(df.head())

def fetch_fuelsource_day(date : datetime) -> pd.DataFrame:
    date_str = date.strftime("%Y%m%d")
    response = re.get(FUELSOURCE_URL_TEMPLATE.format(date=date_str))
    
    if response.status_code != 200:
        # @TODO: some form of logging
        print("Not yet implemented")
        exit()
    
    df = pd.read_csv(StringIO(response.text))

    df.columns = df.columns.str.lower()

    return df

def fetch_fuelsource(start : datetime, end : datetime) -> pd.DataFrame:
    """
    Fetches fuelsource data
    """
date = datetime.strptime("2025-01-01", "%Y-%m-%d")
print(date)
df = fetch_fuelsource_day(date)
print(df.head())