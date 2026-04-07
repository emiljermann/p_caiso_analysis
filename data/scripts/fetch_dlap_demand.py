"""
Fetch hourly demand by CAISO sub-balancing authority (PGE, SCE, SDGE) from the EIA API.

API: EIA v2 electricity/rto/region-sub-ba-data
Output: dlap_demand_{tag}

Usage:
    python fetch_dlap_demand.py --start 2025-01-01 --end 2026-01-01 --tag 2025
"""

import argparse
import json
import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
import re


CREDS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "credentials.json")

with open(CREDS_PATH) as _f:
    creds = json.load(_f)
    API_KEY = creds["eia_api_key"]
    POSTGRES_PASSWORD = creds["postgres_password"]

engine = create_engine(f'postgresql://postgres:{POSTGRES_PASSWORD}@localhost:5432/caiso')

BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"
OUTPUT_DIR = os.path.dirname(__file__)

PARAMS_TEMPLATE = {
    "api_key": API_KEY,
    "frequency": "hourly",
    "data[0]": "value",
    "facets[subba][]": ["PGAE", "SCE", "SDGE"],
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "length": 5000,
}

dlap_mapping = {
    "PGAE" : "PGE",
    "SDGE" : "SDGE",
    "SCE"  : "SCE",
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
    parser = argparse.ArgumentParser(description="Fetch hourly CAISO DLAP demand from the EIA API.")
    parser.add_argument("--start", default="2025-01-01", help="Start date (YYYY-MM-DD), default: 2025-01-01")
    parser.add_argument("--end", default="2026-01-01", help="End date (YYYY-MM-DD), default: 2026-01-01")
    parser.add_argument("--tag", required=True, help="Table suffix, for example: 2025")
    args = parser.parse_args()

    sql_table_name = "dlap_demand" + "_" + args.tag

    records = fetch_all_pages(f"{args.start}T00", f"{args.end}T00")
    df = pd.DataFrame(records)

    df = df[["subba", "period", "value"]].rename(columns={"period":"datetime_utc", "subba":"dlap", "value":"demand_MWh"})
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["dlap"] = df["dlap"].map(dlap_mapping)
    df = df.sort_values(["dlap", "datetime_utc"]).reset_index(drop=True)
    df["state"] = "CA"
    df["iso"] = "CAISO"

    df = df[["state", "iso", "dlap", "datetime_utc", "demand_MWh"]]

    print(f"\nFetched {len(df):,} total records")
    print(f"Columns: {list(df.columns)}")
    print(f"Sub-BAs: {sorted(df['dlap'].unique()) if 'dlap' in df.columns else 'N/A'}")

    csv_path = os.path.join(OUTPUT_DIR, "eia_dlap_demand.csv")
    parquet_path = os.path.join(OUTPUT_DIR, "eia_dlap_demand.parquet")
    # df.to_csv(csv_path, index=False)
    # df.to_parquet(parquet_path, index=False)

    df.to_sql(name = sql_table_name,
              con = engine,
              if_exists = "replace",
              index = False)
    print(f"Saved to:\n  {csv_path}\n  {parquet_path}")


if __name__ == "__main__":
    main()
