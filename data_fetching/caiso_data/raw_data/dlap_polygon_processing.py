import geopandas as gpd

utility_mapping = {
    "Pacific Gas & Electric Company" : "PGE",
    "San Diego Gas & Electric"       : "SDGE",
    "Southern California Edison"     : "SCE"
}

gdf = gpd.read_file("ElectricLoadServingEntities_IOU_POU_-3184854433875766149.geojson")
sub_cols = ["Utility", "geometry"]
gdf_sub = gdf.loc[:,sub_cols]

gdf_sub["dlap"] = gdf_sub["Utility"].map(utility_mapping)
gdf_sub.dropna(inplace=True, ignore_index=True)
gdf_sub = gdf_sub[["dlap", "geometry"]]

gdf_sub.to_file("dlap_polygons.gpkg")