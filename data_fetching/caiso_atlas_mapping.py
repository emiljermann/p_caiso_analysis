"""
caiso_atlas_mapping.py — Atlas Reference: TAC Area and Trading Hub PNode Mappings
----------------------------------------------------------------------------------
Pulls two OASIS Atlas reference datasets and derives a TAC Area → Trading Hub
mapping from their overlap.

Atlas query names (confirmed from OASIS Interface Specification, Section 5):
  ATL_PNODE_MAP   — Map of all PNodes to each Trading Hub APNode
  ATL_TAC_AREA    — Map of all PNodes to each TAC Area

WORKFLOW
--------
Step 1 — Run test pulls to inspect raw column names:
    python caiso_atlas_mapping.py --test-hub
    python caiso_atlas_mapping.py --test-tac

Step 2 — Fill in the column name globals below based on what you see:
    HUB_PNODE_COL, HUB_APNODE_COL
    TAC_PNODE_COL, TAC_AREA_COL

Step 3 — Run the full pipeline:
    python caiso_atlas_mapping.py

    Or re-derive from already-saved files (no API calls):
    python caiso_atlas_mapping.py --combine-only
"""

from __future__ import annotations

import argparse
from datetime import date

import pandas as pd

from caiso_oasis_utils import (
    OUTPUT_DIR,
    oasis_datetime,
    fetch_oasis_csv,
    save,
)

# ---------------------------------------------------------------------------
# COLUMN NAME GLOBALS
# ---------------------------------------------------------------------------
# After running --test-hub and --test-tac, fill these in with the exact
# column names printed in the output. Defaults are best-guess only.

# ATL_PNODE_MAP columns
HUB_PNODE_COL  = "PNODE_ID"    # PNode identifier column in the hub map
HUB_APNODE_COL = "APNODE_ID"   # Trading Hub APNode column in the hub map

# ATL_TAC_AREA columns
TAC_PNODE_COL = "PNODE_ID"       # PNode identifier column in the TAC map
TAC_AREA_COL  = "TAC_AREA_ID"  # TAC Area name column in the TAC map

# ---------------------------------------------------------------------------
# Reference date — Atlas reports require a date window but return current
# network model state. One day is sufficient; no chunking needed.
# ---------------------------------------------------------------------------

DEFAULT_REF_DATE = date(2025, 1, 1)

# ---------------------------------------------------------------------------
# Known Trading Hubs. Used in `extract_hub_short()` to separate trading hub
# from sub trading hub.
# ---------------------------------------------------------------------------

KNOWN_TRADING_HUBS = {"NP15", "SP15", "ZP26"}


# ---------------------------------------------------------------------------
# Step 1a — Test pull: PNode → Trading Hub
# ---------------------------------------------------------------------------

def test_hub_map(ref_date: date) -> None:
    """
    Pull ATL_PNODE_MAP and print a diagnostic so you can identify
    the correct column names to set in HUB_PNODE_COL and HUB_APNODE_COL.
    Does not save any files.
    """
    print(f"\n=== TEST: ATL_PNODE_MAP (PNode → Trading Hub) | ref date: {ref_date} ===")

    params = {
        "queryname":     "ATL_PNODE_MAP",
        "startdatetime": oasis_datetime(ref_date),
        "enddatetime":   oasis_datetime(ref_date, end_of_day=True),
#        "pnode_id":      "ALL",
    }
    df = fetch_oasis_csv(params, label="test_hub_map")

    if df is None or df.empty:
        print("  No data returned. Possible causes:")
        print("  - queryname ATL_PNODE_MAP may be incorrect")
        print("  - pnode_id=ALL may not be a valid parameter — try omitting it")
        return

    print(f"\n  Total rows:  {len(df):,}")
    print(f"\n  All columns ({len(df.columns)}):")
    for col in df.columns:
        print(f"    {col}")
    print(f"\n  First 10 rows:")
    print(df.head(10).to_string())
    print(f"\n  Unique values in each column (up to 10 shown):")
    for col in df.columns:
        vals = df[col].dropna().unique()
        preview = list(vals[:10])
        print(f"    {col}: {preview}{'...' if len(vals) > 10 else ''}")
    print(
        "\n  ACTION: Set HUB_PNODE_COL and HUB_APNODE_COL at the top of this"
        " file, then run --test-tac."
    )


# ---------------------------------------------------------------------------
# Step 1b — Test pull: PNode → TAC Area
# ---------------------------------------------------------------------------

def test_tac_map(ref_date: date) -> None:
    """
    Pull ATL_TAC_AREA_MAP and print a diagnostic so you can identify
    the correct column names to set in TAC_PNODE_COL and TAC_AREA_COL.
    Does not save any files.
    """
    print(f"\n=== TEST: ATL_TAC_AREA_MAP (PNode → TAC Area) | ref date: {ref_date} ===")

    params = {
        "queryname":     "ATL_TAC_AREA_MAP",
        "startdatetime": oasis_datetime(ref_date),
        "enddatetime":   oasis_datetime(ref_date, end_of_day=True),
    }
    df = fetch_oasis_csv(params, label="test_tac_map")

    if df is None or df.empty:
        print("  No data returned. Possible causes:")
        print("  - queryname ATL_TAC_AREA may be incorrect")
        print("  - pnode_id=ALL may not be a valid parameter — try omitting it")
        return

    print(f"\n  Total rows:  {len(df):,}")
    print(f"\n  All columns ({len(df.columns)}):")
    for col in df.columns:
        print(f"    {col}")
    print(f"\n  First 10 rows:")
    print(df.head(10).to_string())
    print(f"\n  Unique values in each column (up to 10 shown):")
    for col in df.columns:
        vals = df[col].dropna().unique()
        preview = list(vals[:10])
        print(f"    {col}: {preview}{'...' if len(vals) > 10 else ''}")
    print(
        "\n  ACTION: Set TAC_PNODE_COL and TAC_AREA_COL at the top of this"
        " file, then run the full pipeline."
    )


# ---------------------------------------------------------------------------
# Step 2 — Full pull: PNode → Trading Hub
# ---------------------------------------------------------------------------

def pull_pnode_hub_map(ref_date: date) -> pd.DataFrame:
    """
    Pull and save ATL_PNODE_MAP using the column names set in globals.
    """
    print(f"\n=== ATL_PNODE_MAP (PNode → Trading Hub) | ref date: {ref_date} ===")
    print(f"  Using columns: PNode={HUB_PNODE_COL!r}, APNode={HUB_APNODE_COL!r}")

    params = {
        "queryname":     "ATL_PNODE_MAP",
        "startdatetime": oasis_datetime(ref_date),
        "enddatetime":   oasis_datetime(ref_date, end_of_day=True),
#        "pnode_id":      "ALL",
    }
    df = fetch_oasis_csv(params, label="pnode_hub_map")

    if df is None or df.empty:
        print("  No data returned.")
        return pd.DataFrame()

    _validate_columns("ATL_PNODE_MAP", df, [HUB_PNODE_COL, HUB_APNODE_COL])
    return save([df], "atlas_pnode_hub_map")


# ---------------------------------------------------------------------------
# Step 3 — Full pull: PNode → TAC Area
# ---------------------------------------------------------------------------

def pull_pnode_tac_map(ref_date: date) -> pd.DataFrame:
    """
    Pull and save ATL_TAC_AREA_MAP using the column names set in globals.
    """
    print(f"\n=== ATL_TAC_AREA_MAP (PNode → TAC Area) | ref date: {ref_date} ===")
    print(f"  Using columns: PNode={TAC_PNODE_COL!r}, TAC Area={TAC_AREA_COL!r}")

    params = {
        "queryname":     "ATL_TAC_AREA_MAP",
        "startdatetime": oasis_datetime(ref_date),
        "enddatetime":   oasis_datetime(ref_date, end_of_day=True),
    }
    df = fetch_oasis_csv(params, label="pnode_tac_map")

    if df is None or df.empty:
        print("  No data returned.")
        return pd.DataFrame()

    _validate_columns("ATL_TAC_AREA", df, [TAC_PNODE_COL, TAC_AREA_COL])
    return save([df], "atlas_pnode_tac_map")


# ---------------------------------------------------------------------------
# Step 4 — Derive TAC Area → Trading Hub mapping
# ---------------------------------------------------------------------------

def derive_tac_to_hub(
    hub_map_df: pd.DataFrame,
    tac_map_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join ATL_PNODE_MAP and ATL_TAC_AREA on PNode ID to produce a
    TAC Area → Trading Hub mapping.

    Uses the four column name globals set at the top of this file.

    For each (TAC_AREA_NAME, TRADING_HUB) pair, counts the number of
    shared PNodes. The dominant hub per TAC area is the one with the most
    PNodes. Split TAC areas (e.g. PG&E across NP15 and ZP26) are retained
    with full counts so the caller can decide how to handle them.

    Returns a DataFrame with columns:
        TAC_AREA_NAME       — e.g. 'PG&E-TAC'
        TRADING_HUB         — full APNode name e.g. 'TH_NP15_GEN-APND'
        TRADING_HUB_SHORT   — e.g. 'NP15'
        PNODE_COUNT         — PNodes in this TAC area mapping to this hub
        TOTAL_PNODES_IN_TAC — total PNodes in the TAC area across all hubs
        PROP_OF_TAC          — PNODE_COUNT / TOTAL_PNODES_IN_TAC
        IS_DOMINANT         — True for the hub with the most PNodes
    """
    if hub_map_df.empty or tac_map_df.empty:
        print("  Cannot derive mapping — one or both input DataFrames are empty.")
        return pd.DataFrame()

    # Normalise to working copies using the globals
    hub = hub_map_df[[HUB_PNODE_COL, HUB_APNODE_COL]].copy()
    hub.columns = ["PNODE_ID", "TRADING_HUB"]
    hub["PNODE_ID"]    = hub["PNODE_ID"].astype(str).str.strip().str.upper()
    hub["TRADING_HUB"] = hub["TRADING_HUB"].astype(str).str.strip()

    tac = tac_map_df[[TAC_PNODE_COL, TAC_AREA_COL]].copy()
    tac.columns = ["PNODE_ID", "TAC_AREA_NAME"]
    tac["PNODE_ID"]      = tac["PNODE_ID"].astype(str).str.strip().str.upper()
    tac["TAC_AREA_NAME"] = tac["TAC_AREA_NAME"].astype(str).str.strip()
    tac["TAC_AREA_NAME"] = tac["TAC_AREA_NAME"].apply(_strip_tac_prefix)

    # Join on PNode ID
    joined = tac.merge(hub, on="PNODE_ID", how="inner")

    print(f"\n  Join results:")
    print(f"    TAC map PNodes:    {tac['PNODE_ID'].nunique():,}")
    print(f"    Hub map PNodes:    {hub['PNODE_ID'].nunique():,}")
    print(f"    Matched PNodes:    {joined['PNODE_ID'].nunique():,}")
    print(
        f"    TAC-only (no hub): "
        f"{tac['PNODE_ID'].nunique() - joined['PNODE_ID'].nunique():,}"
        f"  (load PNodes, interties — expected)"
        f"  (likely load PNodes that are not considered by APNodes)"
    )
    print(
        f"    Hub-only (no TAC): "
        f"{hub['PNODE_ID'].nunique() - joined['PNODE_ID'].nunique():,}"
        f"  (external APNodes — expected)"
        f"  (likely PNodes outside of California but still considered in APNodes)"
    )

    def extract_hub_short(apnode: str) -> str:
        """
        Extract the canonical trading hub name from an APNode string.
        Looks for a known hub token anywhere in the underscore-delimited parts.
        e.g. TH_NP15_GEN-APND → NP15
            TH_NP15_12        → NP15
            NP15              → NP15
        Returns the original string unchanged if no known hub token is found,
        so unexpected APNode formats are visible rather than silently dropped.
        """
        for part in apnode.upper().split("_"):
            if part in KNOWN_TRADING_HUBS:
                return part
        return apnode

    joined["TRADING_HUB_SHORT"] = joined["TRADING_HUB"].apply(extract_hub_short)

    # Count PNodes per (TAC Area, Trading Hub)
    # e.g. :
    # TAC_AREA_NAME  | TRADING_HUB_SHORT | PNODE_COUNT
    # PG&E-TAC       | NP15              | 312
    # PG&E-TAC       | ZP26              | 47
    # SCE-TAC        | SP15              | 198
    counts = (
        joined.groupby(["TAC_AREA_NAME", "TRADING_HUB_SHORT"])
        .agg(PNODE_COUNT=("PNODE_ID", "nunique"))
        .reset_index()
    )

    # Add total PNodes per TAC area and percentage share per hub
    # TAC_AREA_NAME  | TRADING_HUB_SHORT | PNODE_COUNT | TOTAL_PNODES_IN_TAC | PROP_OF_TAC
    # PG&E-TAC       | NP15              | 312         | 359                 | 0.869
    # PG&E-TAC       | ZP26              | 47          | 359                 | 0.131
    tac_totals = (
        counts.groupby("TAC_AREA_NAME")["PNODE_COUNT"]
        .sum()
        .rename("TOTAL_PNODES_IN_TAC")
    )
    counts = counts.merge(tac_totals, on="TAC_AREA_NAME")
    counts["PROP_OF_TAC"] = (
        counts["PNODE_COUNT"] / counts["TOTAL_PNODES_IN_TAC"]
    ).round(3)

    # Flag the hub with the most PNodes per TAC area
    # TAC_AREA_NAME  | TRADING_HUB_SHORT | PNODE_COUNT | IS_DOMINANT
    # PG&E-TAC       | NP15              | 312         | True
    # PG&E-TAC       | ZP26              | 47          | False
    dominant_idx = counts.groupby("TAC_AREA_NAME")["PNODE_COUNT"].idxmax()
    counts["IS_DOMINANT"] = False
    counts.loc[dominant_idx, "IS_DOMINANT"] = True

    counts = counts.sort_values(
        ["TAC_AREA_NAME", "PNODE_COUNT"], ascending=[True, False]
    ).reset_index(drop=True)

    out = OUTPUT_DIR / "atlas_tac_to_hub"
    counts.to_parquet(out.with_suffix(".parquet"), index=False)
    counts.to_csv(out.with_suffix(".csv"), index=False)
    print(f"\n  [save] TAC→Hub mapping: {len(counts):,} rows → {out}.parquet")

    return counts


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_mapping_report(tac_to_hub_df: pd.DataFrame) -> None:
    """
    Print a human-readable summary of the derived TAC → Hub mapping,
    and emit a ready-to-paste TAC_TO_HUB dict for caiso_source_mix.py.
    """
    if tac_to_hub_df.empty:
        print("  No mapping to report.")
        return

    print("\n" + "=" * 70)
    print("TAC AREA → TRADING HUB MAPPING REPORT")
    print("=" * 70)

    for tac, group in tac_to_hub_df.groupby("TAC_AREA_NAME"):
        total  = group["TOTAL_PNODES_IN_TAC"].iloc[0]
        n_hubs = len(group)
        split_flag = "  *** SPLIT ***" if n_hubs > 1 else ""
        print(f"\n  {tac}  (total PNodes with hub assignment: {total}){split_flag}")
        for _, row in group.iterrows():
            dominant_marker = " ← dominant" if row["IS_DOMINANT"] else ""
            print(
                f"    {row['TRADING_HUB_SHORT']:8s}  "
                f"{row['PNODE_COUNT']:4d} PNodes  "
                f"({row['PROP_OF_TAC']:5.1f}%)"
                f"{dominant_marker}"
            )

    splits = (
        tac_to_hub_df.groupby("TAC_AREA_NAME")
        .filter(lambda g: len(g) > 1)["TAC_AREA_NAME"]
        .unique()
    )
    if len(splits) > 0:
        print(f"\n  TAC areas spanning multiple hubs: {sorted(splits)}")
        print(
            "  NOTE: These cannot be assigned to a single hub without information\n"
            "  loss. Consider splitting load MW proportionally by PNode count,\n"
            "  or assigning to the dominant hub with a documented caveat."
        )
    else:
        print("\n  No TAC areas split across multiple hubs.")

    print("\n" + "=" * 70)
    print("\n# ---- Paste into caiso_source_mix.py ----")
    print("# Dominant hub = most PNodes. Split TAC areas noted in comments.")
    print("TAC_TO_HUB = {")
    dominant = tac_to_hub_df[tac_to_hub_df["IS_DOMINANT"]]
    for _, row in dominant.sort_values("TAC_AREA_NAME").iterrows():
        split_note = ""
        group = tac_to_hub_df[tac_to_hub_df["TAC_AREA_NAME"] == row["TAC_AREA_NAME"]]
        if len(group) > 1:
            others = group[~group["IS_DOMINANT"]][["TRADING_HUB_SHORT", "PROP_OF_TAC"]]
            parts  = [f"{r.TRADING_HUB_SHORT} {r.PROP_OF_TAC}%" for r in others.itertuples()]
            split_note = f"  # SPLIT: also {', '.join(parts)}"
        print(
            f'    "{row["TAC_AREA_NAME"]}": "{row["TRADING_HUB_SHORT"]}",  '
            f'# {row["PNODE_COUNT"]} PNodes ({row["PROP_OF_TAC"]}%){split_note}'
        )
    print("}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _strip_tac_prefix(tac_area: str) -> str:
    """
    Remove the 'TAC_' prefix from a TAC area ID if present.
    e.g. 'TAC_ECNTR' → 'ECNTR'
         'TAC_NORTH'  → 'NORTH'
         'PG&E-TAC'   → 'PG&E-TAC'  (unchanged — no TAC_ prefix)
    """
    prefix = "TAC_"
    if tac_area.upper().startswith(prefix):
        return tac_area[len(prefix):]
    return tac_area

def _validate_columns(report_name: str, df: pd.DataFrame, expected: list[str]) -> None:
    """Warn if any expected column is missing from df."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        print(f"\n  WARNING: Expected column(s) not found in {report_name}: {missing}")
        print(f"  Actual columns: {list(df.columns)}")
        print(
            "  Re-run with --test-hub or --test-tac and update the column "
            "globals at the top of this file."
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Fetch CAISO Atlas PNode mappings and derive TAC Area → Trading Hub table.\n\n"
            "Typical workflow:\n"
            "  1. python caiso_atlas_mapping.py --test-hub\n"
            "  2. python caiso_atlas_mapping.py --test-tac\n"
            "  3. Edit HUB_PNODE_COL, HUB_APNODE_COL, TAC_PNODE_COL, TAC_AREA_COL\n"
            "  4. python caiso_atlas_mapping.py\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=DEFAULT_REF_DATE,
        help="Reference date for Atlas query (default: %(default)s).",
    )
    parser.add_argument(
        "--test-hub",
        action="store_true",
        help="Print raw ATL_PNODE_MAP columns and sample rows, then exit.",
    )
    parser.add_argument(
        "--test-tac",
        action="store_true",
        help="Print raw ATL_TAC_AREA columns and sample rows, then exit.",
    )
    parser.add_argument(
        "--combine-only",
        action="store_true",
        help=(
            "Skip API calls. Re-derive mapping from already-saved "
            "atlas_pnode_hub_map.parquet and atlas_pnode_tac_map.parquet."
        ),
    )
    args = parser.parse_args()

    # ── Test modes ───────────────────────────────────────────────────────────
    if args.test_hub:
        test_hub_map(args.date)
        raise SystemExit(0)

    if args.test_tac:
        test_tac_map(args.date)
        raise SystemExit(0)

    # ── Combine-only mode ────────────────────────────────────────────────────
    if args.combine_only:
        hub_path = OUTPUT_DIR / "atlas_pnode_hub_map.parquet"
        tac_path = OUTPUT_DIR / "atlas_pnode_tac_map.parquet"
        missing  = [str(p) for p in [hub_path, tac_path] if not p.exists()]
        if missing:
            print(
                "ERROR: Saved file(s) not found — run without --combine-only first:\n"
                + "\n".join(f"  {p}" for p in missing)
            )
            raise SystemExit(1)
        hub_df = pd.read_parquet(hub_path)
        tac_df = pd.read_parquet(tac_path)
        print(f"  Loaded hub map: {len(hub_df):,} rows")
        print(f"  Loaded TAC map: {len(tac_df):,} rows")

    # ── Full pipeline ────────────────────────────────────────────────────────
    else:
        hub_df = pull_pnode_hub_map(args.date)
        tac_df = pull_pnode_tac_map(args.date)

    if hub_df.empty or tac_df.empty:
        print(
            "\nWARNING: One or both raw maps are empty — cannot derive mapping.\n"
            "Run --test-hub and/or --test-tac to diagnose."
        )
        raise SystemExit(1)

    mapping = derive_tac_to_hub(hub_df, tac_df)
    print_mapping_report(mapping)
