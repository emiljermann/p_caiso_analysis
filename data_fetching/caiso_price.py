"""
caiso_price.py — Dimension 3: Wholesale Price (LMP)
-----------------------------------------------------
Pulls 15-minute real-time predispatch LMP by subLAP zone,
aggregates to hourly, and maps subLAP prefixes to utility
service territories.

Confirmed spec (OASIS Interface Specification v4.3.5):
  queryname      : PRC_RTPD_LMP
  market_run_id  : RTM
  geography      : subLAP zones (27 zones)
  time resolution: 15-minute intervals → aggregated to hourly

LMP = Locational Marginal Price in $/MWh
    = energy component + congestion component + loss component
    (+ GHG component if version=2)

This is WHOLESALE price only. Retail rates (what customers pay on their
bill) are set by utilities under CPUC jurisdiction and are not in OASIS.

subLAP → Utility territory mapping:
  subLAP names encode their utility via prefix:
  PGAE_* → Pacific Gas & Electric      (NP15 / ZP26 territory)
  SCE_*  → Southern California Edison  (SP15 territory)
  SDGE_* → San Diego Gas & Electric    (SP15 territory)
  SMUD_* → Sacramento Municipal Utility District
  TIDC_* → Turlock Irrigation District
  VEA_*  → Valley Electric Association
  IID_*  → Imperial Irrigation District
  LDWP_* → Los Angeles Department of Water & Power

Note on chunk size:
  OASIS spec v4.3.5 states a maximum download window of one hour
  for PRC_RTPD_LMP. We use 1-day chunks which contain 96 intervals
  each — test with 1-day chunks first and reduce if OASIS returns errors.

Usage
-----
    python caiso_price.py                        # full project range, all subLAPs
    python caiso_price.py --test                 # one week only
    python caiso_price.py --node PGAE_APND       # single aggregate node
"""

import argparse
from datetime import date

import pandas as pd

from caiso_oasis_utils import (
    PROJECT_START, PROJECT_END,
    date_chunks, oasis_datetime,
    fetch_oasis_csv, save,
)


# ---------------------------------------------------------------------------
# subLAP → Utility mapping
# ---------------------------------------------------------------------------

def sublap_to_utility(node_name: str) -> str:
    """
    Extract the utility short code from a subLAP node name.
    The prefix before the first underscore is the utility identifier.
    e.g. 'PGAE_PGF1' → 'PGAE', 'SCE_APND' → 'SCE'
    """
    if not isinstance(node_name, str):
        return "UNKNOWN"
    return node_name.split("_")[0].upper()


UTILITY_DISPLAY_NAMES = {
    "PGAE":  "Pacific Gas & Electric",
    "SCE":   "Southern California Edison",
    "SDGE":  "San Diego Gas & Electric",
    "SMUD":  "Sacramento Municipal Utility District",
    "TIDC":  "Turlock Irrigation District",
    "VEA":   "Valley Electric Association",
    "IID":   "Imperial Irrigation District",
    "LDWP":  "Los Angeles Dept. of Water & Power",
}

# Aggregate PNode names — one per major utility, useful for lighter pulls
AGGREGATE_NODES = {
    "PGAE_APND": "Pacific Gas & Electric",
    "SCE_APND":  "Southern California Edison",
    "SDGE_APND": "San Diego Gas & Electric",
    "SMUD_APND": "Sacramento Municipal Utility District",
}

# Chunk size — OASIS spec v4.3.5 notes max 1-hour window for PRC_RTPD_LMP.
# 1-day chunks (96 intervals) are attempted first. If OASIS returns errors
# or empty responses, reduce to sub-day chunks.
# At 6s per request, a full year ≈ 365 requests ≈ 37 minutes per node.
CHUNK_DAYS = 1


# ---------------------------------------------------------------------------
# Pull
# ---------------------------------------------------------------------------

def pull_lmp_rtpd(
    start: date,
    end:   date,
    node:  str = "ALL",
) -> pd.DataFrame:
    """
    Pull 15-minute real-time predispatch LMP for subLAP zones.

    Parameters
    ----------
    node : 'ALL' returns all subLAP zones (27 zones, large dataset).
           Pass a specific node name (e.g. 'PGAE_APND') for a lighter pull.
           Pass 'AGGREGATES' to pull only the 4 major utility aggregate points.

    Returned columns include (at minimum):
        INTERVALSTARTTIME_GMT  — 15-min interval start in GMT
        NODE                   — subLAP or PNode name
        MW                     — LMP in $/MWh
        LMP_TYPE               — LMP | MCE (congestion) | MCC | MCL (loss)
    """
    if node == "AGGREGATES":
        # Pull each aggregate node separately and combine
        frames = []
        for agg_node in AGGREGATE_NODES:
            df = pull_lmp_rtpd(start, end, node=agg_node)
            if not df.empty:
                frames.append(df)
        if frames:
            combined = pd.concat(frames, ignore_index=True).drop_duplicates()
            combined.to_parquet(
                (lambda p: p.with_suffix(".parquet"))(
                    __import__("pathlib").Path("caiso_data") / "lmp_rtpd_aggregates_15min"
                ),
                index=False,
            )
            return combined
        return pd.DataFrame()

    print(f"\n=== LMP RTPD (PRC_RTPD_LMP / RTM | node={node}) | {start} → {end} ===")
    frames = []

    for chunk_start, chunk_end in date_chunks(start, end, CHUNK_DAYS):
        params = {
            "queryname":     "PRC_RTPD_LMP",
            "market_run_id": "RTM",
            "node":          node,
            "startdatetime": oasis_datetime(chunk_start),
            "enddatetime":   oasis_datetime(chunk_end, end_of_day=True),
        }
        df = fetch_oasis_csv(
            params,
            label=f"lmp {chunk_start}–{chunk_end}",
        )
        if df is not None:
            frames.append(df)

    node_label = node.lower().replace("_", "")
    return save(frames, f"lmp_rtpd_{node_label}_15min")


# ---------------------------------------------------------------------------
# Aggregate to hourly + map to utility territory
# ---------------------------------------------------------------------------

def aggregate_to_hourly(lmp_15min_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate 15-minute LMP data to hourly by averaging the four
    15-minute intervals within each clock hour.

    Also adds UTILITY_SHORT and UTILITY_LONG columns by decoding
    the subLAP node name prefix.

    Input must have columns: INTERVALSTARTTIME_GMT, NODE, MW, LMP_TYPE
    (column names as returned by PRC_RTPD_LMP).

    Returns hourly DataFrame with columns:
        INTERVAL_START_GMT     — hour start (UTC-aware)
        NODE                   — subLAP zone name
        UTILITY_SHORT          — e.g. 'PGAE', 'SCE', 'SDGE'
        UTILITY_LONG           — e.g. 'Pacific Gas & Electric'
        LMP_AVG                — average LMP over the hour ($/MWh)
        LMP_MIN                — minimum 15-min LMP in the hour
        LMP_MAX                — maximum 15-min LMP in the hour
        LMP_TYPE               — kept for filtering (use 'LMP' for total price)
    """
    df = lmp_15min_df.copy()

    # Identify columns flexibly
    time_col = next((c for c in df.columns
                     if "INTERVALSTART" in c.upper()), None)
    node_col = next((c for c in df.columns if "NODE" in c.upper()), None)
    mw_col   = next((c for c in df.columns if c.upper() in ("MW", "VALUE")), None)
    type_col = next((c for c in df.columns if "LMP_TYPE" in c.upper() or "TYPE" in c.upper()), None)

    if not all([time_col, node_col, mw_col]):
        raise ValueError(
            f"Cannot identify required columns. Found: {list(df.columns)}"
        )

    df["INTERVAL_START_GMT"] = pd.to_datetime(df[time_col], utc=True)

    group_cols = ["NODE", "UTILITY_SHORT"]
    if type_col:
        group_cols.append("LMP_TYPE")

    df["NODE"] = df[node_col].astype(str)
    df["UTILITY_SHORT"] = df["NODE"].apply(sublap_to_utility)
    df["UTILITY_LONG"]  = df["UTILITY_SHORT"].map(UTILITY_DISPLAY_NAMES).fillna(df["UTILITY_SHORT"])

    hourly = (
        df.groupby(
            [pd.Grouper(key="INTERVAL_START_GMT", freq="h"), "NODE",
             "UTILITY_SHORT", "UTILITY_LONG"]
            + ([type_col] if type_col else [])
        )[mw_col]
        .agg(LMP_AVG="mean", LMP_MIN="min", LMP_MAX="max")
        .reset_index()
        .rename(columns={type_col: "LMP_TYPE"} if type_col else {})
    )
    hourly[["LMP_AVG", "LMP_MIN", "LMP_MAX"]] = (
        hourly[["LMP_AVG", "LMP_MIN", "LMP_MAX"]].round(4)
    )

    out_path = __import__("pathlib").Path("caiso_data") / "lmp_by_utility_hourly"
    hourly.to_parquet(out_path.with_suffix(".parquet"), index=False)
    hourly.to_csv(out_path.with_suffix(".csv"), index=False)
    print(f"  [save] Hourly LMP: {len(hourly):,} rows → {out_path.with_suffix('.parquet')}")

    return hourly


def summarise_by_utility(hourly_lmp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Further aggregate subLAP-level hourly LMP to utility territory level.
    Takes a simple mean across subLAPs within each utility per hour.

    Use this for the shared-geography panel where price is mapped
    to the same utility territory polygons as load and source mix.

    Returns DataFrame with:
        INTERVAL_START_GMT, UTILITY_SHORT, UTILITY_LONG,
        LMP_AVG, LMP_MIN, LMP_MAX, N_SUBLAPS
    """
    # Filter to LMP_TYPE == 'LMP' (total price) if the column exists
    df = hourly_lmp_df.copy()
    if "LMP_TYPE" in df.columns:
        df = df[df["LMP_TYPE"].str.upper() == "LMP"]

    result = (
        df.groupby(["INTERVAL_START_GMT", "UTILITY_SHORT", "UTILITY_LONG"])
        .agg(
            LMP_AVG=("LMP_AVG", "mean"),
            LMP_MIN=("LMP_MIN", "min"),
            LMP_MAX=("LMP_MAX", "max"),
            N_SUBLAPS=("NODE", "nunique"),
        )
        .reset_index()
    )
    result[["LMP_AVG", "LMP_MIN", "LMP_MAX"]] = (
        result[["LMP_AVG", "LMP_MIN", "LMP_MAX"]].round(4)
    )

    out_path = __import__("pathlib").Path("caiso_data") / "lmp_by_utility_territory_hourly"
    result.to_parquet(out_path.with_suffix(".parquet"), index=False)
    result.to_csv(out_path.with_suffix(".csv"), index=False)
    print(f"  [save] Utility LMP: {len(result):,} rows → {out_path.with_suffix('.parquet')}")

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull CAISO wholesale price (LMP) data")
    parser.add_argument("--start", type=date.fromisoformat, default=PROJECT_START)
    parser.add_argument("--end",   type=date.fromisoformat, default=PROJECT_END)
    parser.add_argument(
        "--node", default="AGGREGATES",
        help=(
            "Node to pull. Default='AGGREGATES' (4 major utility aggregate points). "
            "'ALL' for all 27 subLAPs (large). Or a specific node e.g. 'PGAE_APND'."
        ),
    )
    parser.add_argument("--test", action="store_true", help="One week only")
    args = parser.parse_args()

    start = PROJECT_START
    end   = date(2025, 1, 7) if args.test else args.end
    if args.test:
        print("--- TEST MODE: one week only ---")

    raw = pull_lmp_rtpd(start, end, node=args.node)

    if not raw.empty:
        print("\nSample raw rows:")
        print(raw.head(8).to_string())
        print("\nColumns:", list(raw.columns))

        hourly = aggregate_to_hourly(raw)
        utility = summarise_by_utility(hourly)

        print("\nSample hourly utility LMP:")
        print(utility.head(10).to_string())
        print("\nUtilities found:", sorted(utility["UTILITY_SHORT"].dropna().unique()))
        print(
            "\nNOTE: These are WHOLESALE prices ($/MWh), not retail rates."
            "\nRetail rates are set by utilities under CPUC jurisdiction."
        )
