"""
Orchestrator: fetch_all -> trim -> hourly downsample + season -> push to Google Sheets.

Auth: uses <repo_root>/google_sa.json (gitignored).
Target: spreadsheet titled 'CAISO Dashboard Data' (must be shared with the SA email).
Tabs: caiso_fuelmix, eia_demand, sgip_mer. Created if missing; cleared and rewritten each run.

Usage:
    python data/scripts/refresh_and_publish.py [--end YYYYMMDD] [--overlap-days N] [--tag TAG]
                                               [--default-start-days N] [--skip-fetch] [--skip-publish]
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime

import gspread
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
REPO_DIR = os.path.abspath(os.path.join(DATA_DIR, ".."))
SA_JSON = os.path.join(REPO_DIR, "google_sa.json")
SHEET_NAME = "CAISO Dashboard Data"
SEASONS_CSV = os.path.join(DATA_DIR, "seasons.csv")

# (csv stem, groupby cols besides datetime_pt, value col)
PIPELINE = [
    ("caiso_fuelmix", ["iso", "output_type"], "output_MWh"),
    ("eia_demand",    ["iso", "dlap"],        "demand_MWh"),
    ("sgip_mer",      ["iso", "dlap"],        "mer_mTCO2MWh"),
]


def run(cmd: list[str]) -> None:
    """Run a subprocess; abort the whole orchestrator if it fails."""
    print(f"\n$ {' '.join(cmd)}")
    rc = subprocess.call(cmd)
    if rc != 0:
        sys.exit(f"\nAborted: command exited with rc={rc}: {' '.join(cmd)}")


def trim_cutoff(end_yyyymmdd: str) -> str:
    """Given end date YYYYMMDD, return cutoff one year earlier as YYYY-MM-DD."""
    end = datetime.strptime(end_yyyymmdd, "%Y%m%d")
    try:
        cut = end.replace(year=end.year - 1)
    except ValueError:  # Feb 29 -> Feb 28 of prior year
        cut = end.replace(year=end.year - 1, day=28)
    return cut.strftime("%Y-%m-%d")


def prepare_df(stem: str, group_cols: list[str], value_col: str, tag: str | None,
               seasons: dict[int, str]) -> pd.DataFrame:
    """Load CSV, floor to hour, mean-aggregate, add season, ISO-string the datetime."""
    suffix = f"_{tag}" if tag else ""
    csv_path = os.path.join(DATA_DIR, f"{stem}{suffix}.csv")
    df = pd.read_csv(csv_path)
    # Floor in UTC to avoid DST ambiguity, then convert to PT for display.
    dt_utc = pd.to_datetime(df["datetime_pt"], utc=True).dt.floor("h")
    df["datetime_pt"] = dt_utc.dt.tz_convert("America/Los_Angeles")

    df = df.groupby(group_cols + ["datetime_pt"], as_index=False)[value_col].mean()
    df["season"] = df["datetime_pt"].dt.month.map(seasons)
    df = df.sort_values(["datetime_pt"] + group_cols).reset_index(drop=True)
    df["datetime_pt"] = df["datetime_pt"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def push(sh, tab: str, df: pd.DataFrame) -> None:
    """Write df to the given worksheet (create if missing), replacing contents."""
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=tab,
            rows=max(len(df) + 50, 1000),
            cols=max(len(df.columns) + 2, 10),
        )
        print(f"  created tab '{tab}'")
    ws.clear()
    out = df.astype(object).where(df.notna(), None)
    values = [df.columns.tolist()] + out.values.tolist()
    ws.update(values=values, range_name="A1")
    print(f"  pushed {len(df):,} rows to tab '{tab}'")


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh CAISO data, trim, and publish to Google Sheets.")
    parser.add_argument("--end", default=None, help="YYYYMMDD. Default: today.")
    parser.add_argument("--overlap-days", type=int, default=7)
    parser.add_argument("--tag", default=None)
    parser.add_argument("--default-start-days", type=int, default=365)
    parser.add_argument("--skip-fetch", action="store_true")
    parser.add_argument("--skip-publish", action="store_true")
    args = parser.parse_args()

    end = args.end or datetime.now().strftime("%Y%m%d")

    # 1. Fetch
    if not args.skip_fetch:
        cmd = [sys.executable, os.path.join(SCRIPT_DIR, "fetch_all.py"),
               "--end", end,
               "--overlap-days", str(args.overlap_days),
               "--default-start-days", str(args.default_start_days)]
        if args.tag:
            cmd += ["--tag", args.tag]
        run(cmd)

    # 2. Trim
    cutoff = trim_cutoff(end)
    print(f"\nTrim cutoff: {cutoff}")
    for stem, *_ in PIPELINE:
        suffix = f"_{args.tag}" if args.tag else ""
        csv_path = os.path.join(DATA_DIR, f"{stem}{suffix}.csv")
        run([sys.executable, os.path.join(SCRIPT_DIR, "trim_csv.py"), csv_path, cutoff])

    # 3. Load + transform
    seasons_df = pd.read_csv(SEASONS_CSV)
    seasons = dict(zip(seasons_df["month"], seasons_df["season"]))
    dfs = {stem: prepare_df(stem, group_cols, value_col, args.tag, seasons)
           for stem, group_cols, value_col in PIPELINE}

    if args.skip_publish:
        print("\nSkipping publish.")
        return 0

    # 4. Publish
    print(f"\nAuthenticating via {SA_JSON}...")
    gc = gspread.service_account(filename=SA_JSON)
    sh = gc.open(SHEET_NAME)
    print(f"Opened spreadsheet: {sh.title}")

    for stem, df in dfs.items():
        push(sh, stem, df)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
