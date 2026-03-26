"""
caiso_source_mix.py — Dimension 2: Source Mix
----------------------------------------------
Derives hourly renewable vs. non-renewable generation fraction
per CAISO trading hub (NP15, SP15, ZP26).

Method (confirmed from OASIS spec v4.3.5):
  Step 1 — Pull actual renewable generation by fuel type and trading hub
            from SLD_REN_FCST (market_run_id=RTM)
  Step 2 — Pull total generation by TAC area from ENE_SLRS (market_run_id=RTM)
  Step 3 — Aggregate ENE_SLRS TAC area totals up to trading hub level
            using TAC_TO_HUB mapping (derived from Atlas pipeline)
  Step 4 — Compute renewable fraction = renewable_MW / total_gen_MW per hub per hour

Geographic resolution of result: 3 trading hubs (NP15 / ZP26 / SP15)
Time resolution of result: hourly

This is the finest source mix resolution available in OASIS.
Fuel type breakdown: Solar, Wind, Geothermal, Biomass, Biogas, Small Hydro
  vs. everything else (gas + nuclear + large hydro combined, unlabelled).

TAC → Hub mapping verified via caiso_atlas_mapping.py (ATL_PNODE_MAP ⨝
ATL_TAC_AREA_MAP). PG&E territory splits ~87% NP15 / ~13% ZP26; all PG&E
TAC entries assigned to NP15 as dominant hub (documented approximation).

Usage
-----
    python caiso_source_mix.py                   # full project range
    python caiso_source_mix.py --test            # one week only
    python caiso_source_mix.py --step 1          # SLD_REN_FCST only
    python caiso_source_mix.py --step 2          # ENE_SLRS only
    python caiso_source_mix.py --combine         # combine already-saved files
"""

import argparse
import re
from datetime import date
from pathlib import Path

import pandas as pd

from caiso_oasis_utils import (
    PROJECT_START, PROJECT_END, OUTPUT_DIR,
    date_chunks, oasis_datetime,
    fetch_oasis_csv, save,
)


# ---------------------------------------------------------------------------
# TAC area → Trading hub mapping
#
# Derived from caiso_atlas_mapping.py (ATL_PNODE_MAP ⨝ ATL_TAC_AREA_MAP).
#
# CAISO officially defines three California TAC areas (per BPM):
#   North (NORTH)       = PG&E territory  → NP15 dominant (~87%), ZP26 minor (~13%)
#   East Central (ECNTR) = SCE territory  → SP15
#   South (SOUTH)       = SDG&E territory → SP15
#
# Atlas also returns NCNTR (8 PNodes, unidentified) — tentatively NP15.
#
# Operational reports (SLD_FCST, ENE_SLRS) may use utility-style TAC area
# names (e.g. "PG&E-TAC", "SCE-TAC") rather than the Atlas identifiers.
# _normalize_tac_name() strips the TAC prefix/suffix before the lookup,
# so TAC_TO_HUB only needs the bare utility identifier as key.
#
# The PG&E / NP15+ZP26 split: Atlas shows ~87% of PG&E PNodes in NP15,
# ~13% in ZP26. We assign all PG&E TAC entries to NP15 as the dominant hub.
# ZP26 has no dedicated TAC area in the CAISO billing structure — it is a
# pricing zone that overlaps PG&E's TAC area. Diablo Canyon and other central
# CA generators are in ZP26 but within PG&E-TAC for billing purposes.
# ---------------------------------------------------------------------------

def _normalize_tac_name(name: str) -> str:
    """
    Remove the word 'TAC' and accompanying special characters from a TAC area name.
    Handles both the Atlas prefix format (TAC_NORTH → NORTH) and the operational
    report suffix format (PG&E-TAC → PG&E, NCPA-TAC → NCPA).
    """
    name = name.strip()
    name = re.sub(r'^TAC[_\s]+', '', name, flags=re.IGNORECASE)   # TAC_NORTH → NORTH
    name = re.sub(r'[_\-\s]+TAC$', '', name, flags=re.IGNORECASE) # PG&E-TAC  → PG&E
    return name.strip()


TAC_TO_HUB: dict[str, str] = {
    # =========================================================================
    # Keys are normalized TAC area names (after _normalize_tac_name strips
    # any TAC_ prefix or -TAC suffix).
    #
    # Atlas-derived identifiers (ATL_TAC_AREA_MAP, confirmed from BPM):
    #   NORTH  = North TAC Area       = PG&E territory
    #   ECNTR  = East Central TAC Area = SCE territory
    #   SOUTH  = South TAC Area       = SDG&E territory
    #   NCNTR  = unknown sub-area (8 PNodes) — tentative NP15
    #
    # Operational report names (SLD_FCST / ENE_SLRS) after stripping suffix:
    #   PG&E / PGAE / PGE → NP15   (PG&E splits ~87% NP15 / ~13% ZP26;
    #                                assigned to NP15 as dominant hub)
    # =========================================================================

    # --- Atlas identifiers ---
    "NORTH":  "NP15",   # PG&E — confirmed via Atlas PNode counts
    "ECNTR":  "SP15",   # SCE  — confirmed via Atlas PNode counts
    "SOUTH":  "SP15",   # SDG&E — confirmed via Atlas PNode counts
    "NCNTR":  "NP15",   # Unknown sub-area, 8 PNodes — tentative

    # --- NP15 (PG&E territory) ---
    "PG&E":   "NP15",
    "PGAE":   "NP15",   # alternate spelling without ampersand
    "PGE":    "NP15",
    "NCPA":   "NP15",   # Northern California Power Agency
    "SVP":    "NP15",   # Silicon Valley Power
    "SMUD":   "NP15",   # Sacramento Municipal Utility District
    "TID":    "NP15",   # Turlock Irrigation District
    "TIDC":   "NP15",
    "BANC":   "NP15",   # Balancing Authority of Northern California

    # --- SP15 (SCE + SDG&E territory) ---
    "SCE":    "SP15",   # Southern California Edison
    "SDG&E":  "SP15",   # San Diego Gas & Electric
    "SDGE":   "SP15",   # alternate spelling without ampersand
    "IID":    "SP15",   # Imperial Irrigation District
    "VEA":    "SP15",   # Valley Electric Association
    "LADWP":  "SP15",   # Los Angeles DWP (not CAISO but sometimes appears)

    # --- External / EIM participants (dropped from CA analysis) ---
    # NWMT, PAC, NEVP, AZPS, PSEI, IPCO, BCHA — not mapped; rows will be
    # dropped with a warning, which is correct behaviour.
}

# Chunk sizes
REN_CHUNK_DAYS  = 7    # SLD_REN_FCST can be dense — use short chunks
SLRS_CHUNK_DAYS = 30   # ENE_SLRS is lighter


# ---------------------------------------------------------------------------
# Step 1 — Renewable generation by fuel type and trading hub
# ---------------------------------------------------------------------------

def pull_renewable_generation(start: date, end: date) -> pd.DataFrame:
    """
    Pull actual renewable generation from SLD_REN_FCST.

    market_run_id=RTD returns actuals (not forecasts).

    Returned columns include (at minimum):
        INTERVALSTARTTIME_GMT
        TRADING_HUB            — NP15 | SP15 | ZP26
        RENEWABLE_TYPE         — SOLAR | WIND | GEOTHERMAL | BIOMASS | BIOGAS | SMALL HYDRO
        DATA_ITEM              — e.g. 'Renewable_Forecast_MW', 'Renewable_Actual_MW'
        VALUE                  — MW

    NOTE: Verify on first run that DATA_ITEM contains an 'actual' label,
    not just forecast values, when market_run_id=RTM is used.

    NOTE: For SLD_REN_FCST: "to ensure a high level of accuracy only Eligible Intermittent Resources (EIR), including 
    those that participate in the Participating Intermittent Resource program (PIRP) are included in the report."
    Some renewable generators are therefore not included in this data. So it may underestimate renewable generation.
    This might include geothermal, small hydro, and biomass

    """
    print(f"\n=== Renewable generation (SLD_REN_FCST / RTD) | {start} → {end} ===")
    frames = []

    for chunk_start, chunk_end in date_chunks(start, end, REN_CHUNK_DAYS):
        params = {
            "queryname":      "SLD_REN_FCST",
            "market_run_id":  "ACTUAL",
            "startdatetime":  oasis_datetime(chunk_start),
            "enddatetime":    oasis_datetime(chunk_end, end_of_day=True)
        }
        df = fetch_oasis_csv(
            params,
            label=f"ren {chunk_start}-{chunk_end}",
        )
        if df is not None:
            frames.append(df)

    return save(frames, "renewable_gen_by_hub_hourly")


# ---------------------------------------------------------------------------
# Step 2 — Total generation by TAC area
# ---------------------------------------------------------------------------

def pull_total_generation(start: date, end: date) -> pd.DataFrame:
    """
    Pull total cleared generation by TAC area from ENE_SLRS.

    market_run_id=RTM for real-time actuals.

    Returned columns include (at minimum):
        INTERVALSTARTTIME_GMT
        TAC_AREA_NAME          — e.g. 'PG&E-TAC', 'SCE-TAC'
        DATA_ITEM              — TOTGENMW | TOTLOADMW
        VALUE                  — MW

    ENE_SLRS has NO fuel type breakdown — it is purely a generation/load
    balance total per TAC area. This is the denominator for the renewable
    fraction calculation.
    """
    print(f"\n=== Total generation (ENE_SLRS / RTM) | {start} → {end} ===")
    frames = []

    for chunk_start, chunk_end in date_chunks(start, end, SLRS_CHUNK_DAYS):
        params = {
            "queryname":      "ENE_SLRS",
            "market_run_id":  "RTM",
            "startdatetime":  oasis_datetime(chunk_start),
            "enddatetime":    oasis_datetime(chunk_end, end_of_day=True)
        }
        df = fetch_oasis_csv(
            params,
            label=f"slrs {chunk_start}–{chunk_end}",
        )
        if df is not None:
            frames.append(df)

    return save(frames, "total_gen_by_tac_hourly")


# ---------------------------------------------------------------------------
# Step 3 + 4 — Aggregate and compute renewable fraction
# ---------------------------------------------------------------------------

def compute_source_mix(
    renewable_df: pd.DataFrame,
    total_gen_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine renewable generation (by hub) with total generation (by TAC area)
    to produce hourly renewable fraction per trading hub.

    Parameters
    ----------
    renewable_df : from pull_renewable_generation()
    total_gen_df : from pull_total_generation()

    Returns
    -------
    DataFrame with columns:
        INTERVAL_START         — hourly timestamp (GMT)
        TRADING_HUB            — NP15 | SP15 | ZP26
        SOLAR_MW               — actual solar generation in hub
        WIND_MW
        GEOTHERMAL_MW
        BIOMASS_MW
        BIOGAS_MW
        SMALL_HYDRO_MW
        TOTAL_RENEWABLE_MW     — sum of above
        TOTAL_GEN_MW           — total generation in hub (from ENE_SLRS)
        NON_RENEWABLE_MW       — TOTAL_GEN_MW - TOTAL_RENEWABLE_MW
        RENEWABLE_PROP          — TOTAL_RENEWABLE_MW / TOTAL_GEN_MW

    ⚠️  The TAC→hub aggregation step uses TAC_TO_HUB defined above.
    Rows whose TAC area is not in TAC_TO_HUB will be dropped with a warning.
    External / EIM TAC areas (NWMT, PAC, NEVP, etc.) are intentionally unmapped.
    """

    # --- Identify column names (OASIS column names can vary slightly) ---

    # Renewable DataFrame
    ren_time_col  = "INTERVALSTARTTIME_GMT"
    ren_hub_col   = "TRADING_HUB"
    ren_type_col  = "RENEWABLE_TYPE"
    ren_val_col   = "MW"

    # Total generation DataFrame
    gen_time_col  = find_col(total_gen_df, "INTERVALSTARTTIME_GMT", "OPR_HR")
    gen_tac_col   = find_col(total_gen_df, "TAC_AREA_NAME", "TAC_AREA")
    gen_item_col  = find_col(total_gen_df, "DATA_ITEM", "XML_DATA_ITEM")
    gen_val_col   = find_col(total_gen_df, "VALUE", "MW")

    # --- Renewable: pivot fuel types to columns, resample to hourly ---
    ren = renewable_df[[ren_time_col, ren_hub_col, ren_type_col, ren_val_col]].copy()
    ren.columns = ["INTERVAL_START", "TRADING_HUB", "RENEWABLE_TYPE", "MW"]
    ren["INTERVAL_START"] = pd.to_datetime(ren["INTERVAL_START"], utc=True)
    # Resample to hourly mean (SLD_REN_FCST RTM may return 15-min intervals)
    ren = (
        ren.groupby([pd.Grouper(key="INTERVAL_START", freq="h"),
                     "TRADING_HUB", "RENEWABLE_TYPE"])["MW"]
        .mean()
        .reset_index()
    )
    # Pivot renewable types to columns
    ren_pivot = ren.pivot_table(
        index=["INTERVAL_START", "TRADING_HUB"],
        columns="RENEWABLE_TYPE",
        values="MW",
        aggfunc="sum",
    ).reset_index()
    # Normalise column names: "SMALL HYDRO" → "SMALL_HYDRO_MW" etc.
    ren_pivot.columns = [
        c if c in ("INTERVAL_START", "TRADING_HUB")
        else c.upper().replace(" ", "_") + "_MW"
        for c in ren_pivot.columns
    ]
    fuel_mw_cols = [c for c in ren_pivot.columns
                    if c.endswith("_MW") and c != "TOTAL_RENEWABLE_MW"]
    ren_pivot["TOTAL_RENEWABLE_MW"] = ren_pivot[fuel_mw_cols].sum(axis=1)

    # --- Total generation: filter to TOTGENMW, map TAC → hub, aggregate ---
    gen = total_gen_df[
        [gen_time_col, gen_tac_col, gen_item_col, gen_val_col]
    ].copy()
    gen.columns = ["INTERVAL_START", "TAC_AREA_NAME", "DATA_ITEM", "MW"]
    gen["INTERVAL_START"] = pd.to_datetime(gen["INTERVAL_START"], utc=True)

    # Keep only total generation rows
    gen = gen[gen["DATA_ITEM"].str.upper() == "TOTGENMW"].copy()

    # Normalize TAC area names (strip TAC_ prefix / -TAC suffix), then map to hub
    gen["TAC_AREA_NORM"] = gen["TAC_AREA_NAME"].apply(_normalize_tac_name)
    gen["TRADING_HUB"] = gen["TAC_AREA_NORM"].map(TAC_TO_HUB)
    unmapped = gen[gen["TRADING_HUB"].isna()][["TAC_AREA_NAME", "TAC_AREA_NORM"]].drop_duplicates()
    if len(unmapped) > 0:
        print(f"\n  ⚠️  WARNING: {len(unmapped)} TAC area(s) not in TAC_TO_HUB mapping — dropped:")
        for _, row in unmapped.sort_values("TAC_AREA_NAME").iterrows():
            print(f"       {row['TAC_AREA_NAME']!r}  (normalized: {row['TAC_AREA_NORM']!r})")
        print("  These are likely external/EIM balancing authorities (expected).")
        print("  If California TAC areas appear here, add the normalized key to TAC_TO_HUB in caiso_source_mix.py.")
    gen = gen.dropna(subset=["TRADING_HUB"])

    # Resample to hourly and aggregate to hub
    gen_hub = (
        gen.groupby([pd.Grouper(key="INTERVAL_START", freq="h"), "TRADING_HUB"])["MW"]
        .sum()
        .reset_index()
        .rename(columns={"MW": "TOTAL_GEN_MW"})
    )

    # --- Join and compute fractions ---
    result = ren_pivot.merge(gen_hub, on=["INTERVAL_START", "TRADING_HUB"], how="outer")
    result["NON_RENEWABLE_MW"] = (
        result["TOTAL_GEN_MW"] - result["TOTAL_RENEWABLE_MW"]
    ).clip(lower=0)
    result["RENEWABLE_PROP"] = (
        result["TOTAL_RENEWABLE_MW"] / result["TOTAL_GEN_MW"]
    ).round(2)

    result = result.sort_values(["INTERVAL_START", "TRADING_HUB"]).reset_index(drop=True)

    # Save
    out_path = OUTPUT_DIR / "source_mix_by_hub_hourly"
    result.to_parquet(out_path.with_suffix(".parquet"), index=False)
    result.to_csv(out_path.with_suffix(".csv"), index=False)
    print(f"\n  [save] Source mix: {len(result):,} rows → {out_path.with_suffix('.parquet')}")

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull CAISO source mix data")
    parser.add_argument("--start",   type=date.fromisoformat, default=PROJECT_START)
    parser.add_argument("--end",     type=date.fromisoformat, default=PROJECT_END)
    parser.add_argument("--test",    action="store_true", help="One week only")
    parser.add_argument("--step",    type=int, choices=[1, 2],
                        help="Run only step 1 (renewables) or step 2 (total gen)")
    parser.add_argument("--combine", action="store_true",
                        help="Combine already-saved step 1 and step 2 files")
    args = parser.parse_args()

    start = args.start
    end   = date(2025, 1, 7) if args.test else args.end
    if args.test:
        print("--- TEST MODE: one week only ---")

    if args.combine:
        ren_path = OUTPUT_DIR / "renewable_gen_by_hub_hourly.parquet"
        gen_path = OUTPUT_DIR / "total_gen_by_tac_hourly.parquet"
        if not ren_path.exists() or not gen_path.exists():
            print("ERROR: saved files not found. Run steps 1 and 2 first.")
        else:
            ren_df = pd.read_parquet(ren_path)
            gen_df = pd.read_parquet(gen_path)
            mix    = compute_source_mix(ren_df, gen_df)
            print(mix.head(10).to_string())
    elif args.step == 1:
        pull_renewable_generation(start, end)
    elif args.step == 2:
        pull_total_generation(start, end)
    else:
        # Default: run both steps and combine
        ren_df = pull_renewable_generation(start, end)
        gen_df = pull_total_generation(start, end)
        if not ren_df.empty and not gen_df.empty:
            mix = compute_source_mix(ren_df, gen_df)
            print("\nSample output:")
            print(mix.head(10).to_string())
            print("\nTrading hubs found:", sorted(mix["TRADING_HUB"].dropna().unique()))
