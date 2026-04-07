"""
Query EIA API v2 metadata for the fueltype facet
on the electricity/rto/fuel-type-data endpoint.

Usage:
    python eia_fueltype_facets.py <YOUR_API_KEY>
"""

import sys
import json
import urllib.request
import os

API_BASE = "https://api.eia.gov/v2"
ROUTE = "electricity/rto/fuel-type-data"

CREDS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "credentials.json")
with open(CREDS_PATH) as _f:
    API_KEY = json.load(_f)["eia_api_key"]

# https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/


def fetch_json(url):
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def main():

    # 1. Get route metadata (shows available facets, frequencies, etc.)
    meta_url = f"{API_BASE}/{ROUTE}?api_key={API_KEY}"
    print(f"Fetching metadata from: {ROUTE}\n")
    meta = fetch_json(meta_url)

    response = meta.get("response", {})

    print(response)

    print("=== Route Description ===")
    print(f"  id:   {response.get('id')}")
    print(f"  name: {response.get('name')}")
    print(f"  desc: {response.get('description')}\n")

    # 2. List available facets
    facets = response.get("facets", [])
    print("=== Available Facets ===")
    for f in facets:
        print(f"  {f.get('id'):20s}  {f.get('description', '')}")

    # # 3. Get the fueltype facet values
    # facet_url = f"{API_BASE}/{ROUTE}/facet/fueltype?api_key={api_key}"
    # print(f"\nFetching fueltype facet values...\n")
    # facet_data = fetch_json(facet_url)

    # facet_response = facet_data.get("response", {})
    # values = facet_response.get("facets", [])

    # print("=== Fuel Type Facet Values ===")
    # print(f"  {'ID':<10} {'Name':<30} {'Alias'}")
    # print(f"  {'-'*10} {'-'*30} {'-'*20}")
    # for v in values:
    #     print(f"  {v.get('id',''):<10} {v.get('name',''):<30} {v.get('alias','')}")

    # print(f"\nTotal fuel types: {len(values)}")


if __name__ == "__main__":
    main()
