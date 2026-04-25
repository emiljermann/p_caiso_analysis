"""
Fetch hourly demand by CAISO DLAP (PGE, SCE, SDGE) from the EIA v2 API.

API: https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/
Output: data/eia_demand[_{tag}].csv with columns [iso, dlap, datetime_pt, demand_MWh]

Usage:
    python fetch_eia_demand.py --start YYYYMMDD --end YYYYMMDD [--tag TAG]
"""

import argparse
import json
import os
import time
from datetime import datetime

import pandas as pd
import requests

BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"
CREDS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "credentials.json")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..")
PAGE_SIZE = 5000
SLEEP_SECONDS = 1

DLAP_MAPPING = {"PGAE": "PGE", "SCE": "SCE", "SDGE": "SDGE"}


def fetch_all_pages(api_key: str, start: str, end: str) -> list[dict]:
    """Paginate through EIA region-sub-ba-data for CAISO DLAPs."""
    all_records = []
    offset = 0
    while True:
        params = {
            "api_key": api_key,
            "frequency": "local-hourly",
            "data[0]": "value",
            "facets[parent][]": "CISO",
            "facets[subba][]": ["PGAE", "SCE", "SDGE"],
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "length": PAGE_SIZE,
            "offset": offset,
        }
        print(f"Fetching offset={offset}...")
        resp = requests.get(BASE_URL, params=params, timeout=60)
        if not resp.ok:
            print(f"  HTTP {resp.status_code} for {resp.url}")
            print(f"  Response body: {resp.text}")
            resp.raise_for_status()
        data = resp.json()

        records = data["response"]["data"]
        total = int(data["response"]["total"])
        all_records.extend(records)
        print(f"  Got {len(records)} records ({len(all_records)}/{total})")

        if len(all_records) >= total or not records:
            break
        offset += len(records)
        time.sleep(SLEEP_SECONDS)
    return all_records


def main():
    parser = argparse.ArgumentParser(description="Fetch CAISO DLAP demand from EIA.")
    parser.add_argument("--start", required=True, help="Start date (YYYYMMDD), Pacific time")
    parser.add_argument("--end", required=True, help="End date (YYYYMMDD), Pacific time, exclusive")
    parser.add_argument("--tag", default=None, help="Optional filename suffix")
    args = parser.parse_args()

    with open(CREDS_PATH) as f:
        api_key = json.load(f)["eia_api_key"]

    # EIA local-hourly requires an offset suffix; -08 (PST) works year-round for this endpoint.
    start = datetime.strptime(args.start, "%Y%m%d").strftime("%Y-%m-%dT00-08")
    end = datetime.strptime(args.end, "%Y%m%d").strftime("%Y-%m-%dT00-08")

    records = fetch_all_pages(api_key, start, end)
    df = pd.DataFrame(records)

    df = df[["subba", "period", "value"]].rename(
        columns={"period": "datetime_pt", "subba": "dlap", "value": "demand_MWh"}
    )
    df["dlap"] = df["dlap"].map(DLAP_MAPPING)
    df["datetime_pt"] = pd.to_datetime(df["datetime_pt"], utc=True).dt.tz_convert(
        "America/Los_Angeles"
    )
    df["demand_MWh"] = pd.to_numeric(df["demand_MWh"], errors="coerce")
    df["iso"] = "CAISO"
    df = df[["iso", "dlap", "datetime_pt", "demand_MWh"]]

    # End date is exclusive: drop any rows at or after midnight PT of the end date.
    end_pt = pd.Timestamp(
        datetime.strptime(args.end, "%Y%m%d"), tz="America/Los_Angeles"
    )
    df = df[df["datetime_pt"] < end_pt]

    df = df.sort_values(["dlap", "datetime_pt"]).reset_index(drop=True)

    filename = f"eia_demand_{args.tag}.csv" if args.tag else "eia_demand.csv"
    out_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(out_path, index=False)
    print(f"\nWrote {len(df):,} rows to {out_path}")
    print(f"DLAPs: {sorted(df['dlap'].dropna().unique())}")
    print(f"Range: {df['datetime_pt'].min()} -> {df['datetime_pt'].max()}")


if __name__ == "__main__":
    main()
