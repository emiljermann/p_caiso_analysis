import geopandas as gpd
import matplotlib.pyplot as plt
from shapely import buffer

url = "Balancing_Authority_4582246882131662236.geojson"

gdf = gpd.read_file(url)

iid_polygon = gdf.where(gdf["NAME"] == "IID").dropna().iloc[0,2].buffer(0.01) # to remove thin strip at bottom of CAISO

gdf = gdf.where(gdf["NAME"] == "CALISO").dropna()
sub_cols = ["NAME", "geometry"]
print(gdf.columns)
gdf = gdf[sub_cols].rename(columns={"NAME":"name"})
print(gdf.columns)
gdf = gdf.iloc[[0]].reset_index(drop=True)
gdf['geometry'] = gdf['geometry'].apply(lambda x: x - iid_polygon)
gdf['state'] = "CA"
gdf['iso'] = "CAISO"
gdf = gdf[["state", "iso", "geometry"]]

print(gdf.head())

gdf.to_file("caiso.geojson")