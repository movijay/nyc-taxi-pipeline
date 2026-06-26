"""
load_data.py
------------
Downloads NYC TLC Yellow Taxi 2023 Parquet files from AWS Open Data Registry
and optionally loads them into DuckDB for local dbt development.

Usage:
  python scripts/load_data.py --output-dir data/ --load-duckdb
"""

import argparse
import os
import urllib.request
from pathlib import Path

MONTHS = range(1, 13)
S3_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", default="data")
parser.add_argument("--load-duckdb", action="store_true",
                    help="Load downloaded files into DuckDB after download")
parser.add_argument("--duckdb-path", default="/tmp/nyc_taxi.duckdb")
args = parser.parse_args()

out_dir = Path(args.output_dir)
out_dir.mkdir(exist_ok=True)

# Download Parquet files
for m in MONTHS:
    fname = f"yellow_tripdata_2023-{m:02d}.parquet"
    url = f"{S3_BASE}/{fname}"
    dest = out_dir / fname
    if dest.exists():
        print(f"  [skip] {fname} already exists")
        continue
    print(f"  [download] {url}")
    urllib.request.urlretrieve(url, dest)

# Download zone lookup CSV
zone_dest = out_dir / "taxi_zone_lookup.csv"
if not zone_dest.exists():
    print(f"  [download] taxi_zone_lookup.csv")
    urllib.request.urlretrieve(ZONE_URL, zone_dest)

# Load into DuckDB
if args.load_duckdb:
    import duckdb
    con = duckdb.connect(args.duckdb_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    parquet_glob = str(out_dir / "yellow_tripdata_2023-*.parquet")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.yellow_trips AS
        SELECT * FROM read_parquet('{parquet_glob}')
    """)
    count = con.execute("SELECT count(*) FROM raw.yellow_trips").fetchone()[0]
    print(f"  Loaded {count:,} rows into raw.yellow_trips ({args.duckdb_path})")
    con.close()

print("Done.")
