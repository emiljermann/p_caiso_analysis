"""
caiso_load.py — Dimension 1: Energy Load
-----------------------------------------
Pulls actual hourly system demand by TAC area using SLD_FCST.

Confirmed spec (OASIS Interface Specification v4.3.5):
  queryname      : SLD_FCST
  market_run_id  : ACTUAL  (posts actual metered demand)
  key data item  : SYS_FCST_ACT_MW
  geography      : TAC area (~20 zones) + CAISO total
  time resolution: Hourly

NOTE: SLD_FCST with market_run_id=ACTUAL is the ONLY confirmed OASIS
dataset that provides actual consumed load at TAC area granularity.
There is no finer geographic resolution for load data in OASIS.

Usage
-----
    python caiso_load.py                         # full project range
    python caiso_load.py --start 2025-01-01 --end 2025-01-07   # custom range
    python caiso_load.py --test                  # one week only, to verify connectivity
"""

import argparse
from datetime import date

from caiso_oasis_utils import (
    PROJECT_START, PROJECT_END,
    date_chunks, oasis_datetime,
    fetch_oasis_csv, save,
)


# SLD_FCST with market_run_id=ACTUAL returns one row per TAC area per hour.
# The execution_type parameter is required when market_run_id=RTM;
# for ACTUAL it is not needed.
#
# Chunk size: 30 days is safe for hourly TAC-area data.
CHUNK_DAYS = 30


def pull_load_actual(start: date, end: date):
    """
    Pull actual hourly system demand by TAC area.

    Returns and saves a DataFrame with (at minimum) columns:
        INTERVALSTARTTIME_GMT   — hour start in GMT
        INTERVALENDTIME_GMT
        TAC_AREA_NAME           — e.g. 'PG&E-TAC', 'SCE-TAC', 'SDG&E-TAC'
        DATA_ITEM               — should be 'SYS_FCST_ACT_MW'
        VALUE                   — MW consumed in that TAC area that hour
        OPR_DT                  — operating date
    """
    print(f"\n=== Load (SLD_FCST / ACTUAL) | {start} → {end} ===")
    frames = []

    for chunk_start, chunk_end in date_chunks(start, end, CHUNK_DAYS):
        params = {
            "queryname":      "SLD_FCST",
            "market_run_id":  "ACTUAL",
            "startdatetime":  oasis_datetime(chunk_start),
            "enddatetime":    oasis_datetime(chunk_end, end_of_day=True),
#           "tac_area_name":  "ALL",
        }
        df = fetch_oasis_csv(
            params,
            label=f"{chunk_start}–{chunk_end}",
        )
        if df is not None:
            frames.append(df)

    return save(frames, "load_actual_hourly")


def pull_load_dam_forecast(start: date, end: date):
    """
    Pull day-ahead market load forecast by TAC area (hourly).

    Useful for comparing forecast vs. actual demand.
    Data item: SYS_FCST_MW (forecast) vs SYS_FCST_ACT_MW (actual).
    """
    print(f"\n=== Load Forecast (SLD_FCST / DAM) | {start} → {end} ===")
    frames = []

    for chunk_start, chunk_end in date_chunks(start, end, CHUNK_DAYS):
        params = {
            "queryname":      "SLD_FCST",
            "market_run_id":  "DAM",
            "startdatetime":  oasis_datetime(chunk_start),
            "enddatetime":    oasis_datetime(chunk_end, end_of_day=True),
#           "tac_area_name":  "ALL",
        }
        df = fetch_oasis_csv(
            params,
            label=f"{chunk_start}–{chunk_end}",
        )
        if df is not None:
            frames.append(df)

    return save(frames, "load_forecast_dam_hourly")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull CAISO load data")
    parser.add_argument("--start", type=date.fromisoformat, default=PROJECT_START)
    parser.add_argument("--end",   type=date.fromisoformat, default=PROJECT_END)
    parser.add_argument(
        "--test", action="store_true",
        help="Pull one week only to verify connectivity and column names"
    )
    args = parser.parse_args()

    if args.test:
        start = PROJECT_START
        end   = date(2025, 1, 7)
        print("--- TEST MODE: one week only ---")
    else:
        start = args.start
        end   = args.end

    df = pull_load_actual(start, end)

    if not df.empty:
        print("\nSample rows:")
        print(df.head(10).to_string())
        print("\nColumns:", list(df.columns))
        print("\nUnique TAC areas found:")
        tac_col = next((c for c in df.columns if "TAC" in c.upper()), None)
        if tac_col:
            print(sorted(df[tac_col].dropna().unique()))
            print("\n", len(df[tac_col].dropna().unique()), " in total.")
        print(
            "\nNOTE: Verify that 'DATA_ITEM' column contains 'SYS_FCST_ACT_MW'."
            "\nIf other DATA_ITEM values appear, filter to SYS_FCST_ACT_MW before analysis."
        )
