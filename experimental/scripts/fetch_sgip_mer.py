"""
Fetch marginal emissions rates (MER) for CAISO DLAPs from the SGIP Signal API.

API: https://sgipsignal.com/sgipmoer
Regions: SGIP_CAISO_PGE, SGIP_CAISO_SCE, SGIP_CAISO_SDGE
Output: data/sgip_mer[_{tag}].csv with columns [iso, dlap, datetime_pt, mer_mTCO2MWh]

Note: SGIP returns kgCO2/kWh. 1 kgCO2/kWh = 1 metric tonne CO2/MWh, so conversion factor is 1.0.

Usage:
    python fetch_sgip_mer.py --start YYYYMMDD --end YYYYMMDD [--tag TAG]
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

BASE_URL = "https://sgipsignal.com"
CREDS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "credentials.json")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..")

DLAPS = ["SGIP_CAISO_PGE", "SGIP_CAISO_SCE", "SGIP_CAISO_SDGE"]
MOER_VERSION = "2.0"
CHUNK_DAYS = 31
TOKEN_REFRESH_MINUTES = 25
SLEEP_SECONDS = 0.5


def login(username: str, password: str) -> str:
    """Authenticate and return a Bearer token."""
    resp = requests.get(
        f"{BASE_URL}/login", auth=HTTPBasicAuth(username, password), timeout=30
    )
    if not resp.ok:
        print(f"  HTTP {resp.status_code} for {resp.url}")
        print(f"  Response body: {resp.text}")
        resp.raise_for_status()
    return resp.json()["token"]


def fetch_chunk(token: str, ba: str, start: str, end: str) -> list[dict]:
    """Fetch MOER data for one region and one <= 31-day window."""
    resp = requests.get(
        f"{BASE_URL}/sgipmoer",
        headers={"Authorization": f"Bearer {token}"},
        params={"ba": ba, "starttime": start, "endtime": end, "version": MOER_VERSION},
        timeout=60,
    )
    if not resp.ok:
        print(f"  HTTP {resp.status_code} for {resp.url}")
        print(f"  Response body: {resp.text}")
        resp.raise_for_status()
    return resp.json()


def fetch_all(username: str, password: str, start_dt: datetime, end_dt: datetime) -> list[dict]:
    """Walk all DLAPs and time chunks; refresh auth token as needed."""
    records = []
    token = login(username, password)
    login_time = time.time()
    print("Logged in.")

    for ba in DLAPS:
        print(f"\n{ba}:")
        chunk_start = start_dt
        while chunk_start < end_dt:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end_dt)

            if (time.time() - login_time) > TOKEN_REFRESH_MINUTES * 60:
                print("  Refreshing token...")
                token = login(username, password)
                login_time = time.time()

            start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%S%z")
            end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S%z")
            print(f"  {start_str} -> {end_str}")
            chunk = fetch_chunk(token, ba, start_str, end_str)
            records.extend(chunk)
            print(f"    Got {len(chunk)} records")

            chunk_start = chunk_end
            time.sleep(SLEEP_SECONDS)
    return records


def main():
    parser = argparse.ArgumentParser(description="Fetch SGIP MOER for CAISO DLAPs.")
    parser.add_argument("--start", required=True, help="Start date (YYYYMMDD), Pacific time")
    parser.add_argument("--end", required=True, help="End date (YYYYMMDD), Pacific time, exclusive")
    parser.add_argument("--tag", default=None, help="Optional filename suffix")
    args = parser.parse_args()

    with open(CREDS_PATH) as f:
        creds = json.load(f)
    username, password = creds["sgip_username"], creds["sgip_password"]

    pt = ZoneInfo("America/Los_Angeles")
    start_dt = datetime.strptime(args.start, "%Y%m%d").replace(tzinfo=pt)
    end_dt = datetime.strptime(args.end, "%Y%m%d").replace(tzinfo=pt)

    records = fetch_all(username, password, start_dt, end_dt)
    df = pd.DataFrame(records)

    df["dlap"] = df["ba"].str.replace("SGIP_CAISO_", "", regex=False)
    df["datetime_pt"] = pd.to_datetime(df["point_time"], utc=True).dt.tz_convert(
        "America/Los_Angeles"
    )
    # 1 kgCO2/kWh == 1 metric tonne CO2/MWh
    df["mer_mTCO2MWh"] = pd.to_numeric(df["moer"], errors="coerce") * 1.0
    df["iso"] = "CAISO"
    df = df[["iso", "dlap", "datetime_pt", "mer_mTCO2MWh"]]

    # End date is exclusive: drop any rows at or after midnight PT of the end date.
    df = df[df["datetime_pt"] < end_dt]

    df = df.sort_values(["dlap", "datetime_pt"]).reset_index(drop=True)

    filename = f"sgip_mer_{args.tag}.csv" if args.tag else "sgip_mer.csv"
    out_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(out_path, index=False)
    print(f"\nWrote {len(df):,} rows to {out_path}")
    print(f"DLAPs: {sorted(df['dlap'].unique())}")
    print(f"Range: {df['datetime_pt'].min()} -> {df['datetime_pt'].max()}")


if __name__ == "__main__":
    main()
