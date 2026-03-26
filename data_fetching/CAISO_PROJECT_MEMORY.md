# CAISO OASIS Data Project — Memory File
*Last updated: 2026-03-24*

---

## Project Goal

Pull hourly energy data for California from 01/01/2025 to 03/22/2026 across three dimensions:
1. **Energy Load** — how much electricity was consumed
2. **Source Mix** — what fraction of generation was renewable vs. non-renewable
3. **Wholesale Price** — what did electricity cost at the wholesale level

Geographic goal: map all three dimensions to lat/lon boundaries that are meaningful to a lay audience. Preferred shared geography is **utility service territory**. County is a secondary option for future use if combining with Census/demographic data.

**Electricity Maps (Tomorrow.co)** is kept as a future option for a physically rigorous consumed fuel mix with cross-border flow tracing. Not used now due to cost for historical data and single-zone California coverage.

---

## API Access

- **Endpoint:** `https://oasis.caiso.com/oasisapi/SingleZip`
- **No authentication required** — all data is public
- **Mandatory parameters:** `queryname`, `startdatetime`, `enddatetime`, `version=1`
- **`market_run_id` is mandatory for operational reports but NOT for Atlas reference reports**
- **Datetime format:** `YYYYMMDDThh:mm-0000` (GMT)
- **Response:** ZIP file containing XML or CSV
- **CSV format code:** `resultformat=6`
- **Bulk historical download:** `https://oasis-bulk.caiso.com/OASISReport` — pre-packaged daily ZIPs, faster for large date ranges
- **Spec document:** https://www.caiso.com/Documents/OASIS-InterfaceSpecification_v4_3_5Clean_Spring2017Release.pdf (v4.3.5, Spring 2017)
- **Newer spec:** v5.1.2 (Fall 2017) available at https://www.caiso.com/library/fall-2017-release-oasis-technical-specifications

---

## Dimension 1 — Energy Load

### Confirmed dataset: `SLD_FCST`
- **Query name:** `SLD_FCST`
- **Market run ID for actuals:** `market_run_id=ACTUAL` (posts actual metered demand)
  - Confirmed via `energy-analytics-project` GitHub repo which has archived this data since 2013 using this exact parameter
  - `tac_area_name` parameter should be **omitted** — API returns all TAC areas by default without it
- **Market run ID for DA forecast:** `market_run_id=DAM`
- **Market run ID for RT forecast:** `market_run_id=RTPD` (15-min)
- **Key data item:** `SYS_FCST_ACT_MW` — actual system demand in MW
- **Geographic granularity:** TAC area (~20 zones) + CAISO total
- **Time granularity:** Hourly for actuals
- **This is the finest available load resolution in OASIS** — no sub-TAC data exists
- **`execution_type` parameter:** only required when `market_run_id=RTM`, not for ACTUAL (spec v4.1.2)

### Confirmed SLD_FCST output columns (verified from test run)
- `INTERVALSTARTTIME_GMT`, `INTERVALENDTIME_GMT` — hour boundaries in GMT; use these as time keys, not OPR_DT
- `LOAD_TYPE` — always 0 for actual demand; ignore
- `OPR_DT` — operating date in **Pacific time**, not GMT; do not use as time key
- `OPR_HR` — operating hour in Pacific time (1–24, hour-ending); do not use as time key
- `OPR_INTERVAL` — always 0 for hourly data; ignore
- `MARKET_RUN_ID` — confirms 'ACTUAL'
- `TAC_AREA_NAME` — geographic zone; includes non-California zones (e.g. AVA = Avista, Washington/Idaho)
- `LABEL` — human-readable description; always "Total Actual Hourly Integrated Load"; ignore
- `XML_DATA_ITEM` — should be `SYS_FCST_ACT_MW`; filter to this value if other items appear
- `POS` — internal CAISO weighting field; always 3.8; ignore
- `MW` — actual load in megawatts; this is the value column
- `EXECUTION_TYPE` — mirrors MARKET_RUN_ID for ACTUAL queries; ignore
- `GROUP` — CSV format grouping artifact; ignore

### ⚠️ Non-California TAC areas in SLD_FCST output
The API returns all TAC areas including external balancing authorities (AVA = Avista, AVRN, etc.).
Filter to California TAC areas before analysis. The `TAC_TO_HUB` mapping in `caiso_source_mix.py`
drops unmapped TAC areas, which handles this automatically.

### Also relevant: `ENE_SLRS`
- **Query name:** `ENE_SLRS`
- **Key data items:** `TOTLOADMW` (total MW cleared as demand), `TOTGENMW` (total MW cleared as generation)
- **Geographic granularity:** TAC area
- **Markets available:** DAM, RUC, HASP, RTM (5-min)
- **No fuel type fields** — purely load/generation balance by zone
- **Use case:** provides total generation denominator per TAC area for source mix calculation

---

## Dimension 2 — Source Mix

### Approach: combine `SLD_REN_FCST` + `ENE_SLRS`
- `SLD_REN_FCST` gives renewable MW by fuel type at **trading hub** level (NP15/SP15/ZP26)
- `ENE_SLRS` gives total generation MW at **TAC area** level
- Aggregate TAC area totals from `ENE_SLRS` up to trading hub level (see TAC→Hub mapping below)
- Divide: renewable MW / total generation MW = renewable fraction per hub per hour
- **Result:** renewable percentage at trading-hub granularity (3 zones), hourly
- **Non-renewable remainder** is gas + nuclear + large hydro combined — no further fuel breakdown available at this resolution in OASIS

### Confirmed dataset: `SLD_REN_FCST`
- **Query name:** `SLD_REN_FCST`
- **Market run IDs:** `DAM` (hourly forecast), `RTPD` (15-min), `RTM` (actuals)
- **Fuel types in `RENEWABLE_TYPE` field:** Solar, Wind, Geothermal, Biomass, Biogas, Small Hydro
- **Geographic granularity:** Trading hub — NP15, SP15, ZP26
- **Time granularity:** Hourly (DAM), 15-min (RTPD/RTM)
- Use `market_run_id=RTM` for actuals matching the load data

### Also relevant: CAISO Daily Renewables Watch
- **URL pattern:** `http://content.caiso.com/green/renewrpt/YYYYMMDD_DailyRenewablesWatch.txt`
- **Format:** Plain text, two tables per file, one file per day
- **Table 1 — Renewable detail (MW):** GEOTHERMAL, BIOMASS, BIOGAS, SMALL HYDRO, WIND TOTAL, SOLAR PV, SOLAR THERMAL
- **Table 2 — Total by resource type (MW):** RENEWABLES, NUCLEAR, THERMAL, IMPORTS, HYDRO
- **Geographic granularity:** ISO-wide only — **no sub-ISO breakdown for non-renewables exists anywhere in public CAISO data**
- **Time granularity:** Hourly (hours 1–24)
- **No rate limit concern** — static file server, not OASIS API. 0.5s pause is sufficient.
- **Caching:** raw .txt files should be saved locally to avoid re-downloading
- This is the **only public CAISO source** with Nuclear, Thermal (gas), and large Hydro generation data
- Use alongside OASIS data: Renewables Watch for ISO-wide full fuel mix; SLD_REN_FCST + ENE_SLRS for geographic renewable fraction

### Known limitation
- Fuel type breakdown only available at trading hub (3 zones) for renewables via SLD_REN_FCST
- Non-renewable fuel type breakdown (nuclear/thermal/hydro) available at ISO-wide level only via Renewables Watch
- Cannot disaggregate hub-level renewable data down to individual TAC areas
- The 3-zone geographic resolution for source mix is a known and acknowledged limitation

---

## Dimension 3 — Wholesale Price (LMP)

### Confirmed dataset: `PRC_RTPD_LMP`
- **Query name:** `PRC_RTPD_LMP`
- **Market run ID:** `RTM`
- **Geographic granularity:** subLAP zones (27 zones) + PNodes (individual generators)
- **Time granularity:** 15-minute intervals
- **Aggregate to hourly** by averaging 4 intervals per hour to match other dimensions
- **LMP components:** energy + congestion + loss (+ GHG component in v2+)
- **This is wholesale price only** — not retail. Retail rates are set by utilities under CPUC jurisdiction and are not in OASIS.

### Also available: `PRC_INTVL_LMP`
- **Query name:** `PRC_INTVL_LMP`
- **Time granularity:** 5-minute RTD (finest available)
- Use if sub-hourly price variation becomes relevant later

### subLAP → Utility mapping
subLAP names self-identify utility via prefix:
- `PGAE_*` → Pacific Gas & Electric (NP15 / ZP26 territory)
- `SCE_*` → Southern California Edison (SP15 territory)
- `SDGE_*` → San Diego Gas & Electric (SP15 territory)
- `SMUD_*` → Sacramento Municipal Utility District
- `TIDC_*` → Turlock Irrigation District
- `VEA_*` → Valley Electric Association
- `IID_*` → Imperial Irrigation District
- `LDWP_*` → Los Angeles Department of Water & Power

### Aggregate PNode names (most useful for utility-level pulls)
- `PGAE_APND` — PG&E aggregated point
- `SCE_APND` — SCE aggregated point
- `SDGE_APND` — SDG&E aggregated point
- `SMUD_APND` — SMUD aggregated point

---

## TAC Area → Trading Hub Mapping

### Conceptual background
TAC areas and trading hubs are distinct constructs built from PNodes for different purposes:

**TAC Areas** are a California-only billing construct defined by CAISO's tariff. They correspond to original utility service territories. Their purpose is to determine who pays transmission access charges — charges apply to anyone using the transmission grid, including both load (consumers) and generators (who use the grid to deliver power to market). TAC areas contain all PNodes (both load and generation) within their territory.

**Trading Hubs** (NP15, SP15, ZP26) are a pricing construct. They are generation-weighted average prices across all generation PNodes within each zone, used to provide a stable wholesale price reference for electricity trading. They extend outside California to include EIM (Energy Imbalance Market) participants in neighboring states. Trading hubs include only generation PNodes, not load PNodes.

### Why PNodes appear in one map but not the other
- **In TAC map only (not hub map):** Load PNodes — metering points for demand — have TAC area assignments for billing but no role in hub price calculation. Also includes some intertie nodes on the load/import side.
- **In hub map only (not TAC map):** EIM participant generators in neighboring states (Nevada, Utah, Oregon, Arizona, etc.) — these get priced relative to CAISO hubs but don't pay California TAC charges. Expected and not a data quality issue.
- **In both maps:** California generation PNodes — these are the correct basis for the TAC→hub mapping.

### Atlas API endpoints for mapping (confirmed query names)
Both are Atlas (reference) reports — no `market_run_id` required, one day date window sufficient:

| Report | Query Name | Description |
|---|---|---|
| PNode → Trading Hub | `ATL_PNODE_MAP` | Maps all PNodes to their Trading Hub APNode |
| PNode → TAC Area | `ATL_TAC_AREA_MAP` | Maps all PNodes to their TAC Area |

**Important:** The spec Section 5 table labels the TAC area report as `ATL_TAC_AREA` but the actual API queryname string is `ATL_TAC_AREA_MAP` (with `_MAP` suffix). Using `ATL_TAC_AREA` returns `INVALID_REQUEST.xml`.

**Sample confirmed URL for ATL_TAC_AREA_MAP (from v3.04 spec):**
```
http://oasis.caiso.com/mrtu-oasis/SingleZip?queryname=ATL_TAC_AREA_MAP&startdate=20061002&enddate=20061002
```
No `pnode_id` parameter — omit it entirely.

**Sample confirmed URL for ATL_PNODE_MAP (from gridstatus source):**
```
http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ATL_PNODE_MAP&version=1&startdatetime=20220801T07:00-0000&enddatetime=20220802T07:00-0000&pnode_id=ALL
```

### TAC area name format
The `ATL_TAC_AREA_MAP` API returns TAC area IDs with a `TAC_` prefix:
```
TAC_ECNTR, TAC_NORTH, TAC_SOUTH, TAC_NWMT, TAC_PAC, TAC_NEVP, TAC_AZPS, TAC_PSEI, TAC_IPCO, TAC_BCHA
```
The `strip_tac_prefix()` function in `caiso_atlas_mapping.py` removes this prefix for standardisation.

### TAC area identifiers — what is confirmed vs. unconfirmed

**Confirmed from CAISO Settlements & Billing BPM:**
Source: `https://bpmcm.caiso.com/BPM%20Document%20Library/Settlements%20and%20Billing/Configuration%20Guides/Pre-Calcs/BPM%20-%20CG%20PC%20High%20Voltage%20Access%20Charge%20and%20Transition%20Charge_5.3.doc`

CAISO officially defines exactly three TAC areas:
1. **North TAC Area (N)** — PG&E territory
2. **East Central TAC Area (EC)** — SCE territory
3. **South TAC Area (S)** — SDG&E territory

The BPM states: *"TAC Areas are defined broadly as the previous significant Control Areas in the CAISO Balancing Authority Area."* New PTOs that joined after 2001 were merged into the appropriate existing TAC area. As of January 1, 2011, TAC area-specific rates were phased out and replaced by a single CAISO Grid-wide rate, but the TAC area structure itself remains in use for capacity obligation allocation and PNode assignment.

**Observed from ATL_TAC_AREA_MAP API output (not confirmed against a spec document):**

The API returns these identifiers as `TAC_AREA_ID` values (after stripping the `TAC_` prefix):
- `NORTH` — almost certainly the North TAC Area (PG&E). The abbreviation N/NORTH is consistent with the BPM.
- `ECNTR` — almost certainly the East Central TAC Area (SCE). EC/ECNTR is consistent with the BPM abbreviation EC.
- `SOUTH` — almost certainly the South TAC Area (SDG&E). S/SOUTH is consistent with the BPM.
- `NCNTR` — **unknown, no source found**. Only 8 PNodes. Possibly a sub-area or a newer PTO entity merged into the North or East Central area. Requires verification — do not assume.

The remaining identifiers in the API output (`NWMT`, `PAC`, `NEVP`, `AZPS`, `PSEI`, `IPCO`, `BCHA`) are external balancing authority identifiers for EIM participants outside California (Montana, PacifiCorp, Nevada Power, Arizona, Puget Sound, Idaho, BC Hydro). These are not California TAC areas.

**⚠️ The mapping from API identifier strings (`NORTH`, `ECNTR`, `SOUTH`) to official TAC area names has not been verified against a CAISO document that explicitly lists these strings.** The inference is reasonable but should be confirmed before relying on it in production code — e.g. by checking the CAISO OASIS UI TAC Area Node Mapping report directly.

### PNode and APNode hierarchy

**PNodes** (Pricing Nodes) are individual physical buses on the transmission grid — a generator, a substation, an intertie point. LMPs are calculated at every PNode individually.

**APNodes** (Aggregate Pricing Nodes) are groupings of PNodes used to produce a single aggregate price for a set of generators or loads. Trading at the individual PNode level is impractical for most participants, so APNodes provide stable reference prices. There are several subtypes:

```
PNode (individual bus)
  └── APNode (aggregate — several subtypes)
        ├── Trading Hub APNode  — generation-weighted average across a zone (NP15/SP15/ZP26)
        ├── LAP APNode          — load-weighted average by utility territory (e.g. PGAE-APND, SCE-APND)
        └── BAA Node APNode     — aggregates across the entire CAISO balancing authority area
```

- **Trading Hub APNodes** aggregate generation PNodes within one of the three geographic pricing zones. Weights are based on historical generation output so high-output generators influence the hub price more. These are the primary wholesale electricity reference prices.
- **LAP APNodes** (Load Aggregation Points) aggregate load PNodes within a utility service territory. Used for retail settlement — what load-serving entities pay. Examples: `PGAE-APND`, `SCE-APND`, `SDGE-APND`.
- **BAA Node APNodes** aggregate across the entire CAISO balancing authority area. Used for ISO-wide settlement and EIM inter-BA transactions where a single CAISO-wide reference price is needed rather than a zone-specific one.

This hierarchy is why `extract_hub_short()` filters for known trading hub tokens (`NP15`, `SP15`, `ZP26`) rather than treating all APNodes equally — `ATL_PNODE_MAP` returns all APNode types and LAP/BAA nodes must be excluded from the TAC→hub mapping.

### Trading Hub APNode name format
APNodes follow the pattern `TH_HUBNAME_SUBTYPE`, e.g.:
- `TH_NP15_GEN-APND`
- `TH_NP15_12` (sub-hub aggregate node, still NP15)
- `TH_SP15_GEN-APND`
- `TH_ZP26_GEN-APND`

The `extract_hub_short()` function searches for known hub tokens (`NP15`, `SP15`, `ZP26`) anywhere in the underscore-delimited parts rather than relying on position, because sub-hub APNodes like `TH_NP15_12` exist and would break a positional approach. Groupby in `derive_tac_to_hub()` uses `TRADING_HUB_SHORT` (not the full APNode name) to correctly collapse sub-hub nodes into a single hub count per TAC area.

### Full Network Model Pricing Node Mapping XLS
- **URL:** `https://www.caiso.com/documents/full-network-model-pricing-node-mapping-reference.xls`
- Updated June 2024 on CAISO's Network and Resource Modeling page
- Maps PNodes to trading hubs at the individual node level — same data the Atlas API provides but in Excel form
- Returns binary — must be downloaded and opened in Excel manually
- The `caiso_atlas_mapping.py` script derives the same information programmatically via the API

### ⚠️ ZP26 caveat
ZP26 is geographically between Path 15 and Path 26 (central California). Some PG&E-territory generation (e.g. Diablo Canyon Nuclear, Morro Bay) is in ZP26, not NP15. The PG&E TAC area may therefore split across NP15 and ZP26. The `caiso_atlas_mapping.py` script handles this explicitly — split TAC areas are flagged with `IS_DOMINANT=False` rows and reported separately.

---

## Geographic Boundary Files

### 1. TAC Area / Utility Service Territory polygons
- **Source:** California Energy Commission (CEC) GIS Open Data Hub
- **URL:** `https://cecgis-caenergy.opendata.arcgis.com`
- **Confirmed dataset ID:** `4d87af4f27054544bb3be7fe03b9cd9c`
  - "2017 California Electric Utility Service Territories & Balancing Authorities"
- **Additional layers:** IOU & POU load serving entities; non-IOU/POU entities (tribal, CCA)
- **Important caveat:** These are utility service territory polygons, not TAC area polygons specifically. The two overlap substantially but are not identical. No confirmed standalone TAC-area-specific polygon layer exists as a public download.
- **Format:** GeoJSON available for direct download

### 2. Trading Hub polygons (NP15 / SP15 / ZP26)
- **No standalone public shapefile confirmed to exist**
- **Derivation approach:** dissolve the TAC area / utility territory polygons by hub membership
  - NP15 = union of NP15-member utility polygons
  - SP15 = union of SCE + SDG&E polygons
  - ZP26 = union of ZP26-member polygons (requires verified TAC→hub mapping first)
- **Alternative:** CEC California Electric Infrastructure interactive map may include hub boundaries but no standalone download confirmed

### 3. California County polygons
- **Source:** US Census Bureau TIGER cartographic boundary files
- **URL:** `https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip`
- **Filter:** `STATEFP == "06"` for California only
- **Key fields:** `NAME` (county name), `GEOID` (FIPS code, e.g. "06037" for LA County)
- **Use case:** future use for combining with Census/demographic data; secondary geography option

### 4. Utility Service Territory polygons
- **Source:** HIFLD (Homeland Infrastructure Foundation-Level Data) via CalEMA ArcGIS
- **URL:** `https://gis-calema.opendata.arcgis.com`
- **Dataset:** "California Electric Utility Service Territory"
- **Format:** GeoJSON
- **Key utilities:** PG&E, SCE, SDG&E, SMUD, LADWP, IID, plus smaller municipals
- **Preferred shared geography** for combining all three dimensions on one map

### 5. CEC Climate Zones (future use / source mix overlay)
- **Source:** CEC GIS Open Data Hub
- **URL:** `https://cecgis-caenergy.opendata.arcgis.com`
- **Confirmed dataset ID:** `ff4b4b452c5842c58c9e7c72c04c5a3e`
- **16 building climate zones** (Title 24) — correlate with solar/wind resource availability
- **Use case:** overlay for generation siting analysis; not needed for current scope but kept for later

---

## Shared Geography Decision

**Chosen shared geography: Utility service territory**

Rationale:
- Load (TAC areas) aggregate to utility territory with no interpolation
- Source mix (trading hubs) correspond roughly to utility territories — NP15 ≈ PG&E, SP15 ≈ SCE + SDG&E
- Price (subLAP names) encode utility territory directly via prefix
- Utility territory is familiar to California residents (they know who their utility is)
- 4–6 recognizable polygons — appropriate for lay audience

County is the secondary option if combining with Census/demographic data later.

---

## API Rate Limits

- **Hard limit:** 1 request per 5 seconds — enforced server-side
- **Violation response:** HTTP 429 status code
- **Source:** CAISO OASIS Release Notes v1.1 (Fall 2015); confirmed by gridstatus reference implementation
- **Safe pause:** 6 seconds between requests (scripts use `REQUEST_PAUSE_SECONDS = 6`)
- **429 retry:** wait 30 seconds, retry up to 3 times (`RETRY_PAUSE_SECONDS = 30`, `MAX_RETRIES = 3`)
- **Per-query date range limits:** vary by report — some cap at 1 day, others at 31 days. If a query returns no data, reduce the date range. `PRC_RTPD_LMP` spec notes a maximum of 1 hour per query; use 1-day chunks and reduce if errors occur.
- **Test run request count:** `--test` (one week) makes ~8–10 requests total across all three dimensions, taking ~60–90 seconds at 6s/request — safe.
- **Atlas reports** (ATL_*) are reference data — one request per pipeline run is sufficient, no chunking needed.

---

## Python Dependencies

```
pip install requests pandas geopandas shapely pyarrow openpyxl gridstatus
```

- `gridstatus` — high-level wrapper for OASIS; handles pagination, retries, timezone
- `geopandas` + `shapely` — spatial joins for boundary assignment
- `pyarrow` — parquet file I/O for efficient storage of large datasets
- `openpyxl` — needed to read the CAISO Master Generating File (.xlsx)

---

## Files Written

| File | Status | Notes |
|---|---|---|
| `caiso_oasis_utils.py` | **Current** | Shared HTTP layer, date chunking, rate limiting, file I/O |
| `caiso_load.py` | **Current** | SLD_FCST/ACTUAL pull; TAC area hourly load |
| `caiso_source_mix.py` | **Current** | SLD_REN_FCST + ENE_SLRS; renewable fraction by hub |
| `caiso_renewables_watch.py` | **Current** | Daily Renewables Watch flat file pull; full ISO-wide fuel mix |
| `caiso_price.py` | **Current** | PRC_RTPD_LMP/RTM; subLAP 15-min LMP → hourly by utility |
| `caiso_run.py` | **Current** | Orchestrator; pulls all 3 dimensions, builds unified panel |
| `caiso_atlas_mapping.py` | **Current** | ATL_PNODE_MAP + ATL_TAC_AREA_MAP; derives TAC→hub mapping |

---

## caiso_atlas_mapping.py — Design Notes

### Workflow
```
--test-hub    → inspect ATL_PNODE_MAP columns, no files saved
--test-tac    → inspect ATL_TAC_AREA_MAP columns, no files saved
              → set column name globals at top of file
(no flag)     → full pipeline: fetch, join, derive, report
--combine-only → re-derive from saved parquets, no API calls
```

### Column name globals (set after running test modes)
```python
HUB_PNODE_COL  = "PNODE_ID"      # PNode identifier in hub map
HUB_APNODE_COL = "APNODE_ID"     # Trading Hub APNode in hub map
TAC_PNODE_COL  = "PNODE_ID"      # PNode identifier in TAC map
TAC_AREA_COL   = "TAC_AREA_ID"   # TAC Area name in TAC map
```

### Output files
| File | Description |
|---|---|
| `atlas_pnode_hub_map.parquet/.csv` | Raw PNode → Trading Hub from ATL_PNODE_MAP |
| `atlas_pnode_tac_map.parquet/.csv` | Raw PNode → TAC Area from ATL_TAC_AREA_MAP |
| `atlas_tac_to_hub.parquet/.csv` | Derived TAC Area → dominant Hub with PNode counts |
| `atlas_unmatched_pnodes.csv` | PNodes that didn't join — includes PNODE_ID_LEN and PNODE_ID_LOWER for formatting inspection |

### Key derived columns in atlas_tac_to_hub
- `TRADING_HUB_SHORT` — e.g. `NP15`, extracted from full APNode name by searching for known hub tokens
- `PNODE_COUNT` — number of matched generation PNodes in this (TAC area, hub) pair
- `TOTAL_PNODES_IN_TAC` — total matched PNodes in the TAC area across all hubs
- `PROP_OF_TAC` — proportion (0–1, not percentage) of TAC area's PNodes in this hub
- `IS_DOMINANT` — True for the hub with the most PNodes per TAC area

### print_mapping_report output
Prints: per-TAC-area hub breakdown with split flagging, split TAC area warnings, unique TAC areas list, unique trading hub APNodes list, and a ready-to-paste `TAC_TO_HUB` dict for `caiso_source_mix.py`.

---

## Known Errors Made (do not repeat)

1. **`SLD_RNUALITIES`** — does not exist. Correct query for actual load by TAC area is `SLD_FCST` with `market_run_id=ACTUAL`.
2. **`ENE_SLRS` has fuel type fields** — false. Purely load/generation balance totals by TAC area.
3. **`ENE_SLRS` has a `RESOURCE_ID` field** — false. Zone-level aggregate data only.
4. **TAC area polygons are directly downloadable as a clean public layer** — unconfirmed. CEC layer is utility service territories, the closest available proxy.
5. **Trading hub polygons exist as a standalone public shapefile** — unconfirmed. Must be derived from TAC/utility polygons.
6. **`ATLTACAREA` and `ATLPNODEMAP` are valid query names** — false. These were fabricated. The correct query names are `ATL_TAC_AREA_MAP` and `ATL_PNODE_MAP`.
7. **`ATL_TAC_AREA` is the correct queryname** — false. The spec Section 5 table uses this as a label but the actual API queryname is `ATL_TAC_AREA_MAP`. Using `ATL_TAC_AREA` returns `INVALID_REQUEST.xml`.
8. **gridstatus uses `market_run_id=ACTUAL` for load** — false. gridstatus uses `market_run_id=DAM` and filters to `CA ISO-TAC`. The `ACTUAL` parameter is confirmed via the `energy-analytics-project` GitHub archive.
9. **`tac_area_name=ALL` is a required/valid parameter for SLD_FCST** — unconfirmed. The API returns all TAC areas without it. Omit this parameter.

---

## Recommended Next Steps

1. ✅ ~~Verify `SLD_FCST` with `market_run_id=ACTUAL` returns per-TAC actuals~~ — confirmed working
2. ✅ ~~Confirm correct querynames for Atlas TAC area and hub mapping~~ — confirmed `ATL_PNODE_MAP` and `ATL_TAC_AREA_MAP`
3. Run `caiso_atlas_mapping.py --test-hub` and `--test-tac` to inspect actual column names, then set globals and run full pipeline to produce verified `TAC_TO_HUB` dict
4. Replace hardcoded `TAC_TO_HUB` in `caiso_source_mix.py` with the output of `caiso_atlas_mapping.py`
5. Verify `SLD_REN_FCST` with `market_run_id=RTM` returns actuals (not just forecasts) before building the source mix calculation
6. Derive trading hub polygons by dissolving utility territory polygons once the TAC→hub mapping is confirmed
