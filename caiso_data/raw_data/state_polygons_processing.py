import geopandas as gpd
import matplotlib.pyplot as plt

url = "tl_2025_us_state.shp"
gdf = gpd.read_file(url)

gdf = gdf.where(gdf["NAME"] == "California").dropna()

sub_cols = ["NAME", "geometry"]
gdf = gdf[sub_cols].rename(columns={"NAME": "state"}).reset_index(drop=True)
gdf.at[0, "state"] = "CA"

print(gdf.head())

gdf.to_file("california.geojson")