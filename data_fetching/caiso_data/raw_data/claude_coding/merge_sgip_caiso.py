"""
Merge all SGIP CAISO marginal emissions rate (MOER) CSV files into a single file.

Input: Individual monthly CSV files per DLAP (PGE, SCE, SDGE) from SGIP_CAISO/
Output: sgip_caiso_mer.csv and sgip_caiso_mer.parquet with columns:
    - DLAP: one of {SDGE, PGE, SCE}
    - datetime: UTC timestamp
    - marginal_emissions_kgco2kwh: MOER v2.0 value
"""

import os
import re
import pandas as pd

SGIP_DIR = os.path.join(os.path.dirname(__file__), "..", "SGIP_CAISO")
OUTPUT_DIR = os.path.dirname(__file__)


def merge_sgip_files() -> pd.DataFrame:
    """Read all SGIP CAISO MOER CSVs and merge into a single DataFrame."""
    pattern = re.compile(r"SGIP_CAISO_(PGE|SCE|SDGE)_\d{4}-\d{2}_ALL_MOER_VERSIONS\.csv")

    frames = []
    for filename in sorted(os.listdir(SGIP_DIR)):
        match = pattern.match(filename)
        if not match:
            continue
        dlap = match.group(1)
        filepath = os.path.join(SGIP_DIR, filename)
        df = pd.read_csv(filepath)
        df = df.rename(columns={
            "timestamp": "datetime",
            "MOER version 2.0": "marginal_emissions_kgCO2kWh",
        })
        df["dlap"] = dlap
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True)
    merged["datetime"] = pd.to_datetime(merged["datetime"], utc=True)
    merged = merged[["dlap", "datetime", "marginal_emissions_kgCO2kWh"]]
    merged = merged.sort_values(["dlap", "datetime"]).reset_index(drop=True)
    return merged


def main():
    df = merge_sgip_files()
    csv_path = os.path.join(OUTPUT_DIR, "sgip_caiso_mer.csv")
    parquet_path = os.path.join(OUTPUT_DIR, "sgip_caiso_mer.parquet")
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    print(f"Merged {len(df):,} rows across dlaps: {sorted(df['dlap'].unique())}")
    print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    print(f"Saved to:\n  {csv_path}\n  {parquet_path}")


if __name__ == "__main__":
    main()
