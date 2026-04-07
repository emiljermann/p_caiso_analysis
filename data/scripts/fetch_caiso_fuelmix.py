"""
Fetch hourly generation by fuel type for CAISO from the EIA API.

API: EIA v2 electricity/rto/fuel-type-data
Output: caiso_fuelmix_{tag}

Usage:
    python fetch_caiso_fuelmix.py --start 2025-01-01 --end 2026-01-01 --tag 2025
"""

import argparse
import json
import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine

CREDS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "credentials.json")
with open(CREDS_PATH) as _f:
    creds = json.load(_f)
    API_KEY = creds["eia_api_key"]
    POSTGRES_PASSWORD = creds["postgres_password"]

engine = create_engine(f'postgresql://postgres:{POSTGRES_PASSWORD}@localhost:5432/caiso')
sql_table_name = "eia_dlap_demand"

BASE_URL = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
OUTPUT_DIR = os.path.dirname(__file__)

PARAMS_TEMPLATE = {
    "api_key": API_KEY,
    "frequency": "hourly",
    "data[0]": "value",
    "facets[respondent][]": "CISO",
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "length": 5000,
}


def fetch_all_pages(start: str, end: str) -> list[dict]:
    """Paginate through the EIA API to collect all records."""
    all_records = []
    offset = 0

    while True:
        params = {**PARAMS_TEMPLATE, "start": start, "end": end, "offset": offset}
        print(f"Fetching offset={offset}...")
        resp = requests.get(BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        records = data["response"]["data"]
        total = int(data["response"]["total"])
        all_records.extend(records)
        print(f"  Got {len(records)} records (total available: {total})")

        if len(all_records) >= total:
            break
        offset += len(records)
        time.sleep(1)

    return all_records


def main():
    parser = argparse.ArgumentParser(description="Fetch hourly CAISO generation by fuel type from the EIA API.")
    parser.add_argument("--start", default="2025-01-01", help="Start date (YYYY-MM-DD), default: 2025-01-01")
    parser.add_argument("--end", default="2026-01-01", help="End date (YYYY-MM-DD), default: 2026-01-01")
    parser.add_argument("--tag", default="2025", required=True, help="Table suffix, default: 2025")
    args = parser.parse_args()

    sql_table_name = "caiso_fuelmix" + "_" + args.tag
    
    records = fetch_all_pages(f"{args.start}T00", f"{args.end}T00")
    df = pd.DataFrame(records)
    print(df.head())
    df = df[["period", "type-name", "value", "fueltype",]].rename(columns={"period" : "datetime_utc", "type-name": "fuel_type", "value" : "generation_MWh", "fueltype" : "fuel_type_id"})
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["generation_MWh"] = pd.to_numeric(df["generation_MWh"])
    df["fuel_type"] = df["fuel_type"].str.lower()
    df = df.sort_values(["datetime_utc"]).reset_index(drop=True)
    df["state"] = "CA"
    df["iso"] = "CAISO"

    df = df[["state", "iso", "datetime_utc", "fuel_type", "generation_MWh"]]

    print(f"\nFetched {len(df):,} total records")
    print(f"Columns: {list(df.columns)}")
    if "fueltype" in df.columns:
        print(f"Fuel types: {sorted(df['fueltype'].unique())}")

    csv_path = os.path.join(OUTPUT_DIR, "eia_fuel_type_gen.csv")
    parquet_path = os.path.join(OUTPUT_DIR, "eia_fuel_type_gen.parquet")
    # df.to_csv(csv_path, index=False)
    # df.to_parquet(parquet_path, index=False)

    df.to_sql(name = sql_table_name,
              con = engine,
              if_exists = "replace",
              index = False)
    print(f"Saved to:\n  {csv_path}\n  {parquet_path}")


if __name__ == "__main__":
    main()
