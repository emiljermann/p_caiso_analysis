"""
Fetch hourly demand by CAISO sub-balancing authority (PGE, SCE, SDGE) from the EIA API.

API: EIA v2 electricity/rto/region-sub-ba-data
Date range: 2025-01-01 to 2026-01-01 (GMT)
Output: eia_dlap_demand.csv and eia_dlap_demand.parquet
"""

import json
import os
import time
import requests
import pandas as pd

CREDS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..", "credentials.json")
with open(CREDS_PATH) as _f:
    API_KEY = json.load(_f)["eia_api_key"]

BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"
OUTPUT_DIR = os.path.dirname(__file__)

PARAMS_TEMPLATE = {
    "api_key": API_KEY,
    "frequency": "hourly",
    "data[0]": "value",
    "facets[subba][]": ["PGAE", "SCE", "SDGE"],
    "start": "2025-01-01T00",
    "end": "2026-01-01T00",
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "length": 5000,
}

dlap_mapping = {
    "PGAE" : "PGE",
    "SDGE" : "SDGE",
    "SCE"  : "SCE",
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
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    print(f"Saved to:\n  {csv_path}\n  {parquet_path}")


if __name__ == "__main__":
    main()
