"""
caiso_renewables_watch.py — Full ISO-wide Fuel Mix
----------------------------------------------------
Downloads CAISO Daily Renewables Watch flat files from:
  http://content.caiso.com/green/renewrpt/YYYYMMDD_DailyRenewablesWatch.txt

This is the only public CAISO source that includes non-renewable
fuel type breakdowns (Nuclear, Thermal/Gas, Large Hydro, Imports).
Geographic granularity is ISO-wide only — there is no sub-ISO
breakdown for non-renewables in any CAISO public dataset.

Each daily file contains two hourly tables:

  Table 1 — Renewable breakdown (MW):
    GEOTHERMAL, BIOMASS, BIOGAS, SMALL HYDRO,
    WIND TOTAL, SOLAR PV, SOLAR THERMAL

  Table 2 — Total production by resource type (MW):
    RENEWABLES, NUCLEAR, THERMAL, IMPORTS, HYDRO

These complement the OASIS data:
  - OASIS SLD_REN_FCST  → renewables by trading hub (geographic)
  - OASIS ENE_SLRS      → total generation by TAC area (geographic)
  - This file           → full fuel breakdown, ISO-wide only

No rate limiting concern: this is a static file server, not the
OASIS API. A 0.5s pause between requests is sufficient.

Usage
-----
    python caiso_renewables_watch.py                  # full project range
    python caiso_renewables_watch.py --test           # one week only
    python caiso_renewables_watch.py --start 2025-06-01 --end 2025-06-30
"""

import argparse
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

from caiso_oasis_utils import PROJECT_START, PROJECT_END, OUTPUT_DIR

BASE_URL    = "http://content.caiso.com/green/renewrpt/{date}_DailyRenewablesWatch.txt"
PAUSE       = 0.5   # static file server, no strict rate limit — 0.5s is polite
RAW_DIR     = OUTPUT_DIR / "renewables_watch_raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Parse one daily file
# ---------------------------------------------------------------------------

def parse_renewables_watch(text: str, file_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse the raw text of a Daily Renewables Watch file into two DataFrames.

    Returns
    -------
    (renewable_df, total_df)

    renewable_df columns:
        DATE, HOUR, GEOTHERMAL_MW, BIOMASS_MW, BIOGAS_MW,
        SMALL_HYDRO_MW, WIND_MW, SOLAR_PV_MW, SOLAR_THERMAL_MW,
        TOTAL_RENEWABLE_MW

    total_df columns:
        DATE, HOUR, RENEWABLES_MW, NUCLEAR_MW, THERMAL_MW,
        IMPORTS_MW, HYDRO_MW, TOTAL_MW
    """
    lines = [l for l in text.strip().splitlines() if l.strip()]

    # Locate the two table headers by searching for known header text
    ren_header_idx   = None
    total_header_idx = None
    for i, line in enumerate(lines):
        upper = line.upper()
        if "GEOTHERMAL" in upper:
            ren_header_idx = i
        if "NUCLEAR" in upper and "THERMAL" in upper:
            total_header_idx = i

    if ren_header_idx is None or total_header_idx is None:
        raise ValueError(
            f"Could not locate expected table headers in file for {file_date}. "
            f"File format may have changed."
        )

    # --- Known column names for each table ---
    # These are stable across years in the Daily Renewables Watch format.
    RENEWABLE_COLUMNS = [
        "GEOTHERMAL", "BIOMASS", "BIOGAS", "SMALL_HYDRO",
        "WIND", "SOLAR_PV", "SOLAR_THERMAL",
    ]
    TOTAL_COLUMNS = [
        "RENEWABLES", "NUCLEAR", "THERMAL", "IMPORTS", "HYDRO",
    ]

    def parse_table(header_idx: int, end_idx: int, known_cols: list[str]) -> pd.DataFrame:
        """
        Parse a table between header_idx and end_idx.

        The file is typically tab-separated, but headers contain multi-word
        names like "SMALL HYDRO" and "WIND TOTAL" that break under naive
        whitespace splitting. Instead of parsing the header line, we use
        the known column names and match them to the numeric values per row.

        Each data row has an hour number (1–24) followed by N numeric values
        matching the N known columns.
        """
        expected_n = len(known_cols)
        rows = []
        for line in lines[header_idx + 1 : end_idx]:
            # Split on whitespace — data values are always single numbers
            parts = line.split()
            if not parts:
                continue
            # First token must be an hour number (1–24)
            try:
                hour = int(parts[0])
            except ValueError:
                continue
            if not (1 <= hour <= 24):
                continue
            values = parts[1:]
            # Take exactly the expected number of values
            # (extra tokens from summary rows or formatting are skipped)
            if len(values) < expected_n:
                continue
            values = values[:expected_n]
            row = {"HOUR": hour}
            for col, val in zip(known_cols, values):
                try:
                    row[col] = float(val)
                except ValueError:
                    row[col] = None
            rows.append(row)
        return pd.DataFrame(rows)

    ren_df   = parse_table(ren_header_idx,   total_header_idx, RENEWABLE_COLUMNS)
    total_df = parse_table(total_header_idx, len(lines),       TOTAL_COLUMNS)

    # Add date
    for df in [ren_df, total_df]:
        df.insert(0, "DATE", file_date)

    # Rename renewable columns to _MW suffix
    ren_rename = {c: f"{c}_MW" for c in RENEWABLE_COLUMNS}
    ren_df = ren_df.rename(columns=ren_rename)

    # Compute total renewable MW
    ren_fuel_cols = [c for c in ren_df.columns if c.endswith("_MW")]
    ren_df["TOTAL_RENEWABLE_MW"] = ren_df[ren_fuel_cols].sum(axis=1)

    # Rename total production columns to _MW suffix
    total_rename = {c: f"{c}_MW" for c in TOTAL_COLUMNS}
    total_df = total_df.rename(columns=total_rename)

    # Compute total MW
    total_fuel_cols = [c for c in total_df.columns if c.endswith("_MW")]
    if total_fuel_cols:
        total_df["TOTAL_MW"] = total_df[total_fuel_cols].sum(axis=1)

    return ren_df, total_df


# ---------------------------------------------------------------------------
# Fetch one day
# ---------------------------------------------------------------------------

def fetch_one_day(d: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Download and parse the Daily Renewables Watch file for a single date.
    Caches the raw .txt file locally so re-runs don't re-download.

    Returns (renewable_df, total_df), both empty on failure.
    """
    cache_path = RAW_DIR / f"{d.strftime('%Y%m%d')}_DailyRenewablesWatch.txt"

    if cache_path.exists():
        text = cache_path.read_text()
    else:
        url = BASE_URL.format(date=d.strftime("%Y%m%d"))
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 404:
                # File not yet published (future date or very recent)
                print(f"  {d} — 404 not found (future date?)")
                return pd.DataFrame(), pd.DataFrame()
            resp.raise_for_status()
            text = resp.text
            cache_path.write_text(text)
        except Exception as e:
            print(f"  {d} — ERROR: {e}")
            return pd.DataFrame(), pd.DataFrame()
        time.sleep(PAUSE)

    try:
        ren_df, total_df = parse_renewables_watch(text, d)
        print(f"  {d} — {len(total_df)} hours parsed")
        return ren_df, total_df
    except Exception as e:
        print(f"  {d} — PARSE ERROR: {e}")
        return pd.DataFrame(), pd.DataFrame()


# ---------------------------------------------------------------------------
# Fetch full date range
# ---------------------------------------------------------------------------

def pull_renewables_watch(
    start: date = PROJECT_START,
    end:   date = PROJECT_END,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Download and parse Daily Renewables Watch files for the full date range.

    Returns
    -------
    (renewable_df, total_df)
    Both are hourly, ISO-wide, covering all days in [start, end].

    Saves to:
        caiso_data/renewables_watch_renewable_detail.parquet / .csv
        caiso_data/renewables_watch_total_by_fuel.parquet / .csv
    """
    print(f"\n=== Daily Renewables Watch | {start} → {end} ===")
    print("Geography: ISO-wide only (no sub-ISO breakdown for non-renewables)\n")

    ren_frames   = []
    total_frames = []

    cur = start
    while cur <= end:
        ren_df, total_df = fetch_one_day(cur)
        if not ren_df.empty:
            ren_frames.append(ren_df)
        if not total_df.empty:
            total_frames.append(total_df)
        cur += timedelta(days=1)

    def save_frames(frames, stem):
        if not frames:
            print(f"  No data for {stem}")
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True).drop_duplicates()
        df.to_parquet(OUTPUT_DIR / f"{stem}.parquet", index=False)
        df.to_csv(OUTPUT_DIR / f"{stem}.csv", index=False)
        print(f"\n  Saved {len(df):,} rows → {OUTPUT_DIR / stem}.parquet")
        return df

    ren_out   = save_frames(ren_frames,   "renewables_watch_renewable_detail")
    total_out = save_frames(total_frames, "renewables_watch_total_by_fuel")

    return ren_out, total_out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download CAISO Daily Renewables Watch files (full ISO-wide fuel mix)"
    )
    parser.add_argument("--start", type=date.fromisoformat, default=PROJECT_START)
    parser.add_argument("--end",   type=date.fromisoformat, default=PROJECT_END)
    parser.add_argument(
        "--test", action="store_true",
        help="One week only — verifies parsing and column names"
    )
    args = parser.parse_args()

    start = PROJECT_START
    end   = date(2025, 1, 7) if args.test else args.end
    if args.test:
        print("--- TEST MODE: one week only ---")

    ren_df, total_df = pull_renewables_watch(start, end)

    if not total_df.empty:
        print("\nSample total fuel mix rows:")
        print(total_df.head(8).to_string())
        print(f"\nColumns — renewable detail: {list(ren_df.columns)}")
        print(f"Columns — total by fuel:     {list(total_df.columns)}")
        print(
            "\nNOTE: These are ISO-wide totals only."
            "\nNuclear, Thermal, and large Hydro are not broken out by"
            "\ngeographic zone in any public CAISO data source."
        )
