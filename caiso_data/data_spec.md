## sgip_caiso_mer
### Marginal Emissions Rates (MER) by hour for each DLAP in CAISO
### Bulk Download: "https://content.sgipsignal.com/"
state := California (Added to all datasets to allow for easy merging)
iso := California Independent System Operator
dlap := Default Load Aggregation Point {PGE, SCE, SDGE}
datetime := Datetime for MER with 5-minute level granularity in UTC
mer_kgCO2kWh := Marginal Emissions Rates in kilograms of CO2 per kiloWatt-hour

## eia_dlap_demand
### Hourly Electricity Demand by DLAP
### API: "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"
state := California
iso := California Independent System Operator
dlap := Default Load Aggregation Point {PGE, SCE, SDGE}
datetime := Datetime for electricity demand with hour level granularity in UTC
demand_MWh := Total electricity demand in MegaWatt-hours

## eia_fuel_type_gen
### Hourly Electricity Generation Type Mix for all of CAISO
### API: "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
state := California
iso := California Independent System Operator
datetime := Datetime for electricity generation with hour level granularity in UTC
fuel_type := Electricity generation type (e.g. Coal, Solar, Natural Gas, ...)
generation_MWh := Total electricity generated in MegaWatt-hours

## caiso.geojson
### "https://cecgis-caenergy.opendata.arcgis.com/datasets/147c83114a3f4ff8a82225e3d6c24857_0/explore?location=37.215615%2C-118.678537%2C5"
state := California
iso := California Independent System Operator
geometry := Polygon or MultiPolygon of CAISO

## california.geojson
### "https://www.census.gov/cgi-bin/geo/shapefiles/index.php"
state := California
iso := California Independent System Operator
geometry := Polygon or MultiPolygon of California State

## dlap.geojson
### "https://cecgis-caenergy.opendata.arcgis.com/datasets/CAEnergy::electric-load-serving-entities-iou-pou"
state := California
iso := California Independent System Operator
dlap := Default Load Aggregation Point {PGE, SCE, SDGE}
geometry := Polygon or MultiPolygon of each DLAP


## Notes
- What is in "other" fuel type for eia_fueltype_gen. 
  - Consider accounting for geothermal, battery, and import power?