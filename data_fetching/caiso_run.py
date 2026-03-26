"""
caiso_run.py — Main Runner
---------------------------
Orchestrates all three data dimensions and assembles a unified
hourly panel keyed on (INTERVAL_START_GMT, UTILITY_SHORT).

Dimensions pulled:
  1. Load       — SLD_FCST / ACTUAL, TAC area, hourly
  2. Source mix — SLD_REN_FCST + ENE_SLRS, trading hub, hourly
  3. Price      — PRC_RTPD_LMP / RTM, subLAP → utility territory, hourly

Output files (in caiso_data/):
  load_actual_hourly.parquet / .csv
  renewable_gen_by_hub_hourly.parquet / .csv
  total_gen_by_tac_hourly.parquet / .csv
  source_mix_by_hub_hourly.parquet / .csv
  lmp_rtpd_aggregates_15min.parquet / .csv
  lmp_by_utility_hourly.parquet / .csv
  lmp_by_utility_territory_hourly.parquet / .csv
  unified_panel_hourly.parquet / .csv

Usage
-----
    # Recommended first run — test connectivity and inspect column names
    python caiso_run.py --test

    # Pull all data for full project date range
    python caiso_run.py

    # Pull one dimension at a time
    python caiso_run.py --only load
    python caiso_run.py --only mix
    python caiso_run.py --only price

    # Assemble unified panel from already-saved files (no new API calls)
    python caiso_run.py --panel-only

Prerequisites
-------------
    pip install requests pandas geopandas shapely pyarrow
    python caiso_boundaries.py   # download boundary files first
"""

import argparse
from datetime import date
from pathlib import Path

import pandas as pd

from caiso_oasis_utils import PROJECT_START, PROJECT_END, OUTPUT_DIR
from caiso_load import pull_load_actual
from caiso_renewables_watch import pull_renewables_watch
from caiso_source_mix import (
    pull_renewable_generation,
    pull_total_generation,
    compute_source_mix,
)
from caiso_price import pull_lmp_rtpd, aggregate_to_hourly, summarise_by_utility


# ---------------------------------------------------------------------------
# Unified panel assembly
# ---------------------------------------------------------------------------

def build_unified_panel(
    load_df:     pd.DataFrame,
    mix_df:      pd.DataFrame,
    price_df:    pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine load, source mix, and price into a single hourly panel
    keyed on (INTERVAL_START_GMT, UTILITY_SHORT).

    Geographic alignment:
      Load  — TAC areas mapped directly to UTILITY_SHORT via TAC_TO_UTILITY
      Mix   — trading hubs mapped to UTILITY_SHORT via HUB_TO_UTILITIES
      Price — subLAP prefix already IS UTILITY_SHORT

    This uses utility service territory as the shared geography,
    as decided in the project design.

    Returns a wide-format DataFrame with one row per (hour, utility).
    """

    # ── TAC area → Utility service territory mapping ─────────────────────
    # Maps SLD_FCST / ENE_SLRS TAC area names directly to utility short codes.
    # Includes both Atlas-derived identifiers (NORTH/ECNTR/SOUTH) and
    # operational report name variants.
    TAC_TO_UTILITY: dict[str, str] = {
        # Atlas identifiers (after stripping TAC_ prefix)
        "NORTH":        "PGAE",     # North TAC Area = PG&E territory
        "ECNTR":        "SCE",      # East Central TAC Area = SCE territory
        "SOUTH":        "SDGE",     # South TAC Area = SDG&E territory
        "NCNTR":        "PGAE",     # Unknown sub-area (8 PNodes) — tentative

        # Operational report name variants
        "PG&E-TAC":     "PGAE",
        "PGAE-TAC":     "PGAE",
        "PGE":          "PGAE",
        "SCE-TAC":      "SCE",
        "SCE":          "SCE",
        "SDG&E-TAC":    "SDGE",
        "SDGE-TAC":     "SDGE",
        "SDGE":         "SDGE",
        "SMUD-TAC":     "SMUD",
        "SMUD":         "SMUD",
        "NCPA-TAC":     "PGAE",     # Northern CA Power Agency — in PG&E territory
        "NCPA":         "PGAE",
        "SVP-TAC":      "PGAE",     # Silicon Valley Power — in PG&E territory
        "SVP":          "PGAE",
        "TID-TAC":      "PGAE",     # Turlock Irrigation District
        "TIDC-TAC":     "PGAE",
        "BANC-TAC":     "PGAE",     # Balancing Authority of Northern CA
        "IID-TAC":      "IID",
        "VEA-TAC":      "VEA",
        "LADWP-TAC":    "LDWP",
    }

    # ── Load: map TAC area → utility short code ──────────────────────────
    tac_col  = next((c for c in load_df.columns if "TAC" in c.upper()), None)
    # SLD_FCST uses 'MW' column (confirmed from test run), not 'VALUE'
    val_col  = next((c for c in load_df.columns if c.upper() in ("MW", "VALUE")), None)
    time_col = next((c for c in load_df.columns if "INTERVALSTART" in c.upper()), None)
    item_col = next((c for c in load_df.columns if "DATA_ITEM" in c.upper()), None)

    if not all([tac_col, val_col, time_col]):
        print("  WARNING: Could not identify required load columns — skipping load dimension.")
        print(f"  Available columns: {list(load_df.columns)}")
        print(f"  Detected: tac_col={tac_col}, val_col={val_col}, time_col={time_col}")
        load_panel = pd.DataFrame()
    else:
        load = load_df.copy()
        load["INTERVAL_START_GMT"] = pd.to_datetime(load[time_col], utc=True)

        # Filter to actual demand data item if multiple items present
        # SLD_FCST uses XML_DATA_ITEM column with value SYS_FCST_ACT_MW
        if item_col and "SYS_FCST_ACT_MW" in load[item_col].values:
            load = load[load[item_col] == "SYS_FCST_ACT_MW"]

        # Map TAC area names to utility short codes
        # Build case-insensitive lookup
        tac_utility_lookup = {k: v for k, v in TAC_TO_UTILITY.items()}
        tac_utility_lookup.update({k.upper(): v for k, v in TAC_TO_UTILITY.items()})
        load["UTILITY_SHORT"] = load[tac_col].map(tac_utility_lookup)

        unmapped_tac = load[load["UTILITY_SHORT"].isna()][tac_col].unique()
        if len(unmapped_tac) > 0:
            print(f"  ⚠️  Load: {len(unmapped_tac)} TAC area(s) not in TAC_TO_UTILITY mapping — dropped:")
            for t in sorted(unmapped_tac):
                print(f"       {t}")
            print("  (External/EIM zones are expected here.)")

        load = load.dropna(subset=["UTILITY_SHORT"])
        load[val_col] = pd.to_numeric(load[val_col], errors="coerce")
        load_panel = (
            load.groupby(["INTERVAL_START_GMT", "UTILITY_SHORT"])[val_col]
            .sum()
            .reset_index()
            .rename(columns={val_col: "LOAD_MW"})
        )

    # ── Source mix: map trading hub → utility short code ─────────────────
    # NP15 ≈ PGAE,  SP15 ≈ SCE + SDGE (split evenly),  ZP26 ≈ PGAE
    # Since SP15 covers both SCE and SDGE, those rows are duplicated.
    # Atlas confirms ZP26 is PG&E territory (~13% of PG&E PNodes).
    HUB_TO_UTILITIES = {
        "NP15": ["PGAE"],
        "ZP26": ["PGAE"],     # confirmed via Atlas PNode analysis
        "SP15": ["SCE", "SDGE"],
    }

    if mix_df.empty:
        mix_panel = pd.DataFrame()
    else:
        mix_rows = []
        for _, row in mix_df.iterrows():
            hub = row.get("TRADING_HUB")
            utilities = HUB_TO_UTILITIES.get(hub, [hub])
            for u in utilities:
                r = row.to_dict()
                r["UTILITY_SHORT"] = u
                mix_rows.append(r)
        mix_panel = pd.DataFrame(mix_rows)
        if "INTERVAL_START" in mix_panel.columns:
            mix_panel = mix_panel.rename(columns={"INTERVAL_START": "INTERVAL_START_GMT"})
        mix_panel["INTERVAL_START_GMT"] = pd.to_datetime(
            mix_panel["INTERVAL_START_GMT"], utc=True
        )

    # ── Price: already keyed on UTILITY_SHORT ────────────────────────────
    price_panel = price_df.copy() if not price_df.empty else pd.DataFrame()

    # ── Merge ─────────────────────────────────────────────────────────────
    key = ["INTERVAL_START_GMT", "UTILITY_SHORT"]

    panel = pd.DataFrame()
    for df, label in [(load_panel, "load"), (mix_panel, "mix"), (price_panel, "price")]:
        if df.empty:
            print(f"  Skipping {label} — no data.")
            continue
        if panel.empty:
            panel = df
        else:
            panel = panel.merge(df, on=key, how="outer")

    if panel.empty:
        print("  No data to assemble into panel.")
        return panel

    # Add utility display name
    UTILITY_LONG = {
        "PGAE": "Pacific Gas & Electric",
        "SCE":  "Southern California Edison",
        "SDGE": "San Diego Gas & Electric",
        "SMUD": "Sacramento Municipal Utility District",
        "TIDC": "Turlock Irrigation District",
        "VEA":  "Valley Electric Association",
        "IID":  "Imperial Irrigation District",
        "LDWP": "Los Angeles Dept. of Water & Power",
    }
    panel["UTILITY_LONG"] = panel["UTILITY_SHORT"].map(UTILITY_LONG).fillna(panel["UTILITY_SHORT"])
    panel = panel.sort_values(["INTERVAL_START_GMT", "UTILITY_SHORT"]).reset_index(drop=True)

    # Save
    out = OUTPUT_DIR / "unified_panel_hourly"
    panel.to_parquet(out.with_suffix(".parquet"), index=False)
    panel.to_csv(out.with_suffix(".csv"), index=False)

    print(f"\n=== Unified panel saved ===")
    print(f"  Rows:    {len(panel):,}")
    print(f"  Columns: {list(panel.columns)}")
    print(f"  Date range: {panel['INTERVAL_START_GMT'].min()} → {panel['INTERVAL_START_GMT'].max()}")
    print(f"  Utilities: {sorted(panel['UTILITY_SHORT'].dropna().unique())}")
    print(f"  File: {out.with_suffix('.parquet')}")

    return panel


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CAISO data pipeline — all three dimensions")
    parser.add_argument("--start", type=date.fromisoformat, default=PROJECT_START)
    parser.add_argument("--end",   type=date.fromisoformat, default=PROJECT_END)
    parser.add_argument(
        "--test", action="store_true",
        help="One week only — use to verify connectivity and inspect column names before full pull"
    )
    parser.add_argument(
        "--only", choices=["load", "mix", "price", "watch"],
        help="Pull only one dimension"
    )
    parser.add_argument(
        "--panel-only", action="store_true",
        help="Assemble unified panel from already-saved files, no API calls"
    )
    args = parser.parse_args()

    start = PROJECT_START
    end   = date(2025, 1, 7) if args.test else args.end
    if args.test:
        print("=" * 60)
        print("TEST MODE — one week only")
        print("Inspect column names and DATA_ITEM values before full pull.")
        print("=" * 60)

    # ── Panel-only mode ───────────────────────────────────────────────────
    if args.panel_only:
        def load_if_exists(stem):
            p = OUTPUT_DIR / f"{stem}.parquet"
            if p.exists():
                return pd.read_parquet(p)
            print(f"  Not found: {p} — run full pipeline first.")
            return pd.DataFrame()

        load_df  = load_if_exists("load_actual_hourly")
        mix_df   = load_if_exists("source_mix_by_hub_hourly")
        price_df = load_if_exists("lmp_by_utility_territory_hourly")
        build_unified_panel(load_df, mix_df, price_df)

    # ── Single dimension ──────────────────────────────────────────────────
    elif args.only == "watch":
        pull_renewables_watch(start, end)

    elif args.only == "load":
        pull_load_actual(start, end)

    elif args.only == "mix":
        ren_df = pull_renewable_generation(start, end)
        gen_df = pull_total_generation(start, end)
        if not ren_df.empty and not gen_df.empty:
            compute_source_mix(ren_df, gen_df)

    elif args.only == "price":
        raw = pull_lmp_rtpd(start, end, node="AGGREGATES")
        if not raw.empty:
            hourly = aggregate_to_hourly(raw)
            summarise_by_utility(hourly)

    # ── Full pipeline ─────────────────────────────────────────────────────
    else:
        print("\n--- Step 1: Energy Load ---")
        load_df = pull_load_actual(start, end)

        print("\n--- Step 2a: Source Mix (OASIS — geographic, renewables only) ---")
        ren_df = pull_renewable_generation(start, end)
        gen_df = pull_total_generation(start, end)
        mix_df = pd.DataFrame()
        if not ren_df.empty and not gen_df.empty:
            mix_df = compute_source_mix(ren_df, gen_df)

        print("\n--- Step 2b: Full Fuel Mix (Renewables Watch — ISO-wide, all fuels) ---")
        pull_renewables_watch(start, end)

        print("\n--- Step 3: Wholesale Price ---")
        raw_lmp  = pull_lmp_rtpd(start, end, node="AGGREGATES")
        price_df = pd.DataFrame()
        if not raw_lmp.empty:
            hourly_lmp = aggregate_to_hourly(raw_lmp)
            price_df   = summarise_by_utility(hourly_lmp)

        print("\n--- Step 4: Unified Panel ---")
        if not any(df.empty for df in [load_df, mix_df, price_df]):
            build_unified_panel(load_df, mix_df, price_df)
        else:
            print("  One or more dimensions returned no data — panel not assembled.")
            print("  Run --test first to verify each dimension individually.")
