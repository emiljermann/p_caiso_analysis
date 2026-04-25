"""
Fetch 5-minute generation by fuel type from CAISO Today's Outlook.

Source: https://www.caiso.com/outlook/history/YYYYMMDD/fuelsource.csv
Output: data/caiso_fuelmix[_{tag}].csv with columns [iso, output_type, datetime_pt, output_MWh]

Usage:
    python fetch_caiso_fuelmix.py --start YYYYMMDD --end YYYYMMDD [--tag TAG]
"""

import argparse
import io
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

CSV_URL_TEMPLATE = "https://www.caiso.com/outlook/history/{date}/fuelsource.csv"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..")
SLEEP_SECONDS = 1


def fetch_day(date: datetime) -> pd.DataFrame | None:
    """Fetch one day's fuelsource CSV and return it in long format."""
    date_str = date.strftime("%Y%m%d")
    url = CSV_URL_TEMPLATE.format(date=date_str)
    print(f"Fetching {date.strftime('%Y-%m-%d')}...")

    resp = requests.get(url, timeout=30)
    if resp.status_code == 404:
        print(f"  No data (404)")
        return None
    if not resp.ok:
        print(f"  HTTP {resp.status_code} for {resp.url}")
        print(f"  Response body: {resp.text}")
        resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    if "Time" not in df.columns:
        print(f"  Warning: 'Time' column missing. Columns: {list(df.columns)}")
        return None

    df["datetime_pt"] = pd.to_datetime(
        date.strftime("%Y-%m-%d") + " " + df["Time"].astype(str)
    )

    fuel_cols = [c for c in df.columns if c not in ("Time", "datetime_pt")]
    df = df.melt(
        id_vars=["datetime_pt"],
        value_vars=fuel_cols,
        var_name="output_type",
        value_name="output_MWh",
    )
    df["output_type"] = df["output_type"].str.lower()
    df["output_MWh"] = pd.to_numeric(df["output_MWh"], errors="coerce")
    print(f"  Got {len(df):,} records ({len(fuel_cols)} fuel types)")
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch CAISO fuel mix from Today's Outlook.")
    parser.add_argument("--start", required=True, help="Start date (YYYYMMDD), Pacific time")
    parser.add_argument("--end", required=True, help="End date (YYYYMMDD), Pacific time, exclusive")
    parser.add_argument("--tag", default=None, help="Optional filename suffix")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y%m%d")
    end_date = datetime.strptime(args.end, "%Y%m%d")

    frames = []
    current = start_date
    while current < end_date:
        df_day = fetch_day(current)
        if df_day is not None:
            frames.append(df_day)
        current += timedelta(days=1)
        if current < end_date:
            time.sleep(SLEEP_SECONDS)

    if not frames:
        print("No data retrieved.")
        return

    df = pd.concat(frames, ignore_index=True)

    df["datetime_pt"] = df["datetime_pt"].dt.tz_localize(
        "America/Los_Angeles", ambiguous="NaT", nonexistent="shift_forward"
    )
    df = df.dropna(subset=["datetime_pt"])

    df["iso"] = "CAISO"
    df = df[["iso", "output_type", "datetime_pt", "output_MWh"]]
    df = df.sort_values(["datetime_pt", "output_type"]).reset_index(drop=True)

    filename = f"caiso_fuelmix_{args.tag}.csv" if args.tag else "caiso_fuelmix.csv"
    out_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(out_path, index=False)
    print(f"\nWrote {len(df):,} rows to {out_path}")
    print(f"Range: {df['datetime_pt'].min()} -> {df['datetime_pt'].max()}")
    print(f"Fuel types: {sorted(df['output_type'].unique())}")


if __name__ == "__main__":
    main()
