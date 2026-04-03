"""
Fetch 5-minute marginal emissions rates (MOER) for CAISO DLAPs from the SGIP Signal API.

API: SGIP GHG Signal (https://sgipsignal.com)
Regions: SGIP_CAISO_PGE, SGIP_CAISO_SCE, SGIP_CAISO_SDGE
Output: sgip_caiso_mer.csv and sgip_caiso_mer.parquet
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

BASE_URL = "https://sgipsignal.com"
OUTPUT_DIR = os.path.dirname(__file__)
CREDS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..", "credentials.json")

DLAPS = ["SGIP_CAISO_PGE", "SGIP_CAISO_SCE", "SGIP_CAISO_SDGE"]
MOER_VERSION = "2.0"
CHUNK_DAYS = 31
TOKEN_REFRESH_MINUTES = 25
SLEEP_TIME = 0.5


def load_credentials() -> dict:
    """Load SGIP credentials from credentials.json."""
    with open(CREDS_PATH) as f:
        creds = json.load(f)
    return creds["sgip_username"], creds["sgip_password"]


def login(username: str, password: str) -> str:
    """Authenticate and return a Bearer token."""
    resp = requests.get(
        f"{BASE_URL}/login",
        auth=HTTPBasicAuth(username, password),
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["token"]
    print("  Logged in, token obtained.")
    return token


def fetch_moer_chunk(token: str, ba: str, start: str, end: str) -> list[dict]:
    """Fetch MOER data for a single region and time window (max 31 days)."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "ba": ba,
        "starttime": start,
        "endtime": end,
        "version": MOER_VERSION,
    }
    resp = requests.get(f"{BASE_URL}/sgipmoer", headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_all(username: str, password: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """Fetch MOER data for all DLAPs across the full date range in 31-day chunks."""
    all_records = []
    token = login(username, password)
    login_time = time.time()

    for ba in DLAPS:
        print(f"\nFetching {ba}...")
        chunk_start = start_date

        while chunk_start < end_date:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end_date)

            # Refresh token if near expiry
            if (time.time() - login_time) > TOKEN_REFRESH_MINUTES * 60:
                print("  Refreshing token...")
                token = login(username, password)
                login_time = time.time()

            start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%S%z")
            end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S%z")
            print(f"  {start_str} to {end_str}...")

            records = fetch_moer_chunk(token, ba, start_str, end_str)
            all_records.extend(records)
            print(f"    Got {len(records)} records")

            chunk_start = chunk_end
            time.sleep(SLEEP_TIME)

    return all_records


def main():
    parser = argparse.ArgumentParser(description="Fetch SGIP MOER data from the SGIP Signal API.")
    parser.add_argument("--start", default="2025-01-01", help="Start date (YYYY-MM-DD), default: 2025-01-01")
    parser.add_argument("--end", default="2026-01-01", help="End date (YYYY-MM-DD), default: 2026-01-01")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    username, password = load_credentials()
    records = fetch_all(username, password, start_date, end_date)

    df = pd.DataFrame(records)
    df = df.rename(columns={"point_time": "datetime_utc", "moer": "mer_kgCO2kWh"})
    df["dlap"] = df["ba"].str.replace("SGIP_CAISO_", "", regex=False)
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["state"] = "CA"
    df["iso"] = "CAISO"

    # Aggregate 5-minute data to hourly (mean) to match EIA dataset granularity.
    # Floor timestamps so 00:00, 00:05, ..., 00:55 all map to 00:00 (start-of-hour).
    df["datetime_utc"] = df["datetime_utc"].dt.floor("h")
    df = df.groupby(["state", "iso", "dlap", "datetime_utc"], as_index=False)["mer_kgCO2kWh"].mean()
    df = df.sort_values(["dlap", "datetime_utc"]).reset_index(drop=True)

    df = df[["state", "iso", "dlap", "datetime_utc", "mer_kgCO2kWh"]]

    print(f"\nFetched {len(df):,} hourly records (aggregated from 5-min data)")
    print(f"DLAPs: {sorted(df['dlap'].unique())}")
    print(f"Date range: {df['datetime_utc'].min()} to {df['datetime_utc'].max()}")

    csv_path = os.path.join(OUTPUT_DIR, "sgip_caiso_mer.csv")
    parquet_path = os.path.join(OUTPUT_DIR, "sgip_caiso_mer.parquet")
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    print(f"Saved to:\n  {csv_path}\n  {parquet_path}")


if __name__ == "__main__":
    main()
