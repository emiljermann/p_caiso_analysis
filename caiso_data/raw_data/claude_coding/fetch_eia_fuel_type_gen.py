"""
Fetch hourly generation by fuel type for CAISO from the EIA API.

API: EIA v2 electricity/rto/fuel-type-data
Date range: 2025-01-01 to 2026-01-01 (GMT)
Output: eia_fuel_type_gen.csv and eia_fuel_type_gen.parquet
"""

import os
import time
import requests
import pandas as pd

API_KEY = "553Ih28VcUOGRuzNM9Qd1QOY3Ntzxh6cznYHkbqz"
BASE_URL = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
OUTPUT_DIR = os.path.dirname(__file__)

PARAMS_TEMPLATE = {
    "api_key": API_KEY,
    "frequency": "hourly",
    "data[0]": "value",
    "facets[respondent][]": "CISO",
    "start": "2025-01-01T00",
    "end": "2026-01-01T00",
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "length": 5000,
}


def fetch_all_pages() -> list[dict]:
    """Paginate through the EIA API to collect all records."""
    all_records = []
    offset = 0

    while True:
        params = {**PARAMS_TEMPLATE, "offset": offset}
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
    records = fetch_all_pages()
    df = pd.DataFrame(records)
    df = df[["period", "type-name", "value"]].rename(columns={"period" : "datetime", "type-name": "fuel_type", "value" : "generation_MWh"})
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df["generation_MWh"] = pd.to_numeric(df["generation_MWh"])
    df["fuel_type"] = df["fuel_type"].str.lower()
    df = df.sort_values(["datetime"]).reset_index(drop=True)

    print(f"\nFetched {len(df):,} total records")
    print(f"Columns: {list(df.columns)}")
    if "fueltype" in df.columns:
        print(f"Fuel types: {sorted(df['fueltype'].unique())}")

    csv_path = os.path.join(OUTPUT_DIR, "eia_fuel_type_gen.csv")
    parquet_path = os.path.join(OUTPUT_DIR, "eia_fuel_type_gen.parquet")
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    print(f"Saved to:\n  {csv_path}\n  {parquet_path}")


if __name__ == "__main__":
    main()
