"""Trim a fetcher CSV in place: drop rows where datetime_pt date < cutoff (PT).

Usage:
    python trim_csv.py <csv_path> <YYYY-MM-DD>
"""
import sys
import pandas as pd

path, cutoff = sys.argv[1], sys.argv[2]
df = pd.read_csv(path)
df = df[df["datetime_pt"] >= cutoff].reset_index(drop=True)
df.to_csv(path, index=False)
print(f"{path}: kept {len(df):,} rows with date >= {cutoff}.")
