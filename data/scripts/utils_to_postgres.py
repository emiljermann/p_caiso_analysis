"""
Sends CAISO spatial data and other utilities (.geojson, .csv) to Postgres server.

Usage:
    python utils_to_postgres.py
"""

import os
import json
from sqlalchemy import create_engine, text
import re
import pandas as pd

file_list = [
    "california.geojson", # California polygon
    "caiso.geojson", # CAISO polygon
    "dlaps.geojson", # DLAP polygons
    "california_cities.csv", # California major city location points (lat, lon)
    "seasons.csv", # Month to season map
]

OUTPUT_DIR = os.path.dirname(__file__)
CREDS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "credentials.json")

with open(CREDS_PATH) as _f:
    POSTGRES_PASSWORD = json.load(_f)["postgres_password"]

engine = create_engine(f'postgresql://postgres:{POSTGRES_PASSWORD}@localhost:5432/caiso')

csv_pattern = r"csv$"

for file_name in file_list:
    file_path = os.path.join(OUTPUT_DIR, "..", file_name)
    if re.search(csv_pattern, file_name):
        file = pd.read_csv(file_path)

        out_name = re.split(r"\.", file_name)[0]

        file.to_sql(name = out_name, 
                    con = engine,
                    if_exists = "replace",
                    index = False)
        
    else :
        with open(file_path, 'r') as file:
            content = ''.join(file.read().splitlines())
        
        out_name = re.split(r"\.", file_name)[0]

        with engine.connect() as conn:
            conn.execute( 
                text(
                    f"DROP TABLE IF EXISTS {out_name}; " \
                    f"CREATE TABLE {out_name} AS " \
                    "WITH data AS (SELECT CAST(:content AS json) AS fc) " \
                    "SELECT " \
                    "   regexp_replace(feat->'properties'->>'state','\\s+', '', 'g') AS state, " \
                    "   regexp_replace(feat->'properties'->>'iso','\\s+', '', 'g') AS iso, " \
                    "   regexp_replace(feat->'properties'->>'dlap','\\s+', '', 'g') AS dlap, " \
                    "   ST_GeomFromGeoJSON(feat->>'geometry') AS geometry " \
                    "FROM ( " \
                    "   SELECT json_array_elements(fc->'features') AS feat " \
                    "   FROM data " \
                    ");"
                ),
                {"content" : content, "out_name" : out_name}
            )

            conn.commit()


